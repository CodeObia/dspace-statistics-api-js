import { Injectable } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { DataSource } from 'typeorm';

@Injectable()
export class SharedService {
  constructor(
    private dataSource: DataSource,
    private readonly httpService: HttpService,
  ) {}

  /**
   * Allow aggregate by country, city and month only
   */
  validateAggregationParam(aggregate: string) {
    return aggregate === 'country' ||
      aggregate === 'city' ||
      aggregate === 'month'
      ? aggregate
      : null;
  }

  /**
   * limit should be between 1 and 100
   */
  validateLimitParam(limit: number) {
    limit = Math.abs(limit);
    limit = isNaN(limit) ? 0 : limit;
    limit = limit > 100 ? 100 : limit;
    return limit > 0 ? limit : 100;
  }

  /**
   * page should be a positive number
   */
  validatePageParam(page: number) {
    page = Number(page);
    return page >= 1 ? page : 1;
  }

  /**
   * Aggregating by month should allow only a duration of 12 months
   */
  validateDateParam(startDate: string, endDate: string, aggregate: string) {
    if (aggregate === 'month') {
      const today = new Date();
      // Set date to 15 to prevent timezone differences which could cause the date to shift to the previous/next day
      today.setDate(15);
      today.setHours(0);
      today.setMinutes(0);
      today.setSeconds(0);
      if (startDate == null && endDate == null) {
        const endDateObject = today;

        endDate = `${endDateObject.toISOString().split('T')[0]}`;

        // Set start date to the previous 12 months
        endDateObject.setMonth(endDateObject.getMonth() - 11);
        startDate = `${endDateObject.toISOString().split('T')[0]}`;
      } else if (startDate == null) {
        const endDateObject = new Date(endDate);
        endDateObject.setDate(15);
        endDateObject.setHours(0);
        endDateObject.setMinutes(0);
        endDateObject.setSeconds(0);

        endDate = `${endDateObject.toISOString().split('T')[0]}`;

        // Set start date to the previous 12 months
        endDateObject.setMonth(endDateObject.getMonth() - 11);
        startDate = `${endDateObject.toISOString().split('T')[0]}`;
      } else if (endDate == null) {
        const startDateObject = new Date(startDate);
        startDateObject.setDate(15);
        startDateObject.setHours(0);
        startDateObject.setMinutes(0);
        startDateObject.setSeconds(0);

        startDate = `${startDateObject.toISOString().split('T')[0]}`;

        // Set end date to the next 12 months or current month (whatever is closer)
        startDateObject.setMonth(startDateObject.getMonth() + 11);
        endDate = `${
          (startDateObject > today ? today : startDateObject)
            .toISOString()
            .split('T')[0]
        }`;
      } else {
        const startDateObject = new Date(startDate);
        startDateObject.setDate(15);
        startDateObject.setHours(0);
        startDateObject.setMinutes(0);
        startDateObject.setSeconds(0);

        startDate = `${startDateObject.toISOString().split('T')[0]}`;

        const endDateObject = new Date(endDate);
        endDateObject.setDate(15);
        endDateObject.setHours(0);
        endDateObject.setMinutes(0);
        endDateObject.setSeconds(0);

        // Set end date to the next 12 months or endDate or current month (whatever is closer)
        startDateObject.setMonth(startDateObject.getMonth() + 11);
        endDate = `${
          (endDateObject > startDateObject
            ? startDateObject > today
              ? today
              : startDateObject
            : endDateObject > today
            ? today
            : endDateObject
          )
            .toISOString()
            .split('T')[0]
        }`;
      }
    }
    return [startDate, endDate];
  }

  /**
   * Get items title metadata field ID
   * @return string
   */
  getTitleMetadataField() {
    let titleMetadataFieldArray =
      process.env.DSPACE_TITLE_METADATA_FIELD.split('.');

    // Default title metadata field, dc.title
    if (titleMetadataFieldArray.length < 2) {
      titleMetadataFieldArray = ['dc', 'title'];
    }
    const schema = titleMetadataFieldArray[0];
    const element = titleMetadataFieldArray[1];
    const qualifier =
      titleMetadataFieldArray.length === 3 ? titleMetadataFieldArray[2] : null;

    const query = this.dataSource
      .createQueryBuilder()
      .select(['metadatafieldregistry.metadata_field_id AS metadata_field_id'])
      .from('metadataschemaregistry', 'metadataschemaregistry')
      .innerJoin(
        'metadatafieldregistry',
        'metadatafieldregistry',
        'metadatafieldregistry.metadata_schema_id = metadataschemaregistry.metadata_schema_id',
      )
      .where('metadataschemaregistry.short_id = :schema', { schema })
      .andWhere('metadatafieldregistry.element = :element', { element });
    if (qualifier != null)
      query.andWhere('metadatafieldregistry.qualifier = :qualifier', {
        qualifier,
      });
    else query.andWhere('metadatafieldregistry.qualifier IS NULL');
    return query.getRawOne();
  }

  /**
   * Get DSpace statistics
   */
  async getStatistics(
    items: any,
    startDate: string,
    endDate: string,
    aggregate: string,
    solrViewsMainKey: string,
    solrDownloadsMainKey: string,
  ) {
    // Define common views query params
    const viewsQueryParams: any = {
      limit: 0,
      query: 'type:2',
      filter: ['-isBot:true', 'statistics_type:view'],
    };
    // Define common downloads query params
    const downloadsQueryParams: any = {
      limit: 0,
      query: 'type:0',
      filter: ['-isBot:true', 'statistics_type:view', 'bundleName:ORIGINAL'],
    };

    const itemsIds = [];
    items.map((item: any) => {
      itemsIds.push(item.uuid);
    });
    viewsQueryParams.filter.push(
      `(${solrViewsMainKey}:${itemsIds.join(` OR ${solrViewsMainKey}: `)})`,
    );
    downloadsQueryParams.filter.push(
      `(${solrDownloadsMainKey}:${itemsIds.join(
        ` OR ${solrDownloadsMainKey}: `,
      )})`,
    );

    viewsQueryParams.facet = {};
    downloadsQueryParams.facet = {};

    viewsQueryParams.facet.id = {
      type: 'terms',
      mincount: 1,
      limit: 1000,
      field: solrViewsMainKey,
    };
    downloadsQueryParams.facet.id = {
      type: 'terms',
      mincount: 1,
      limit: 1000,
      field: solrDownloadsMainKey,
    };
    const facetPivotViews = ['id'];
    const facetPivotDownloads = ['id'];
    if (aggregate === 'country') {
      facetPivotViews.push('country');
      viewsQueryParams.facet.id.facet = {
        country: {
          type: 'terms',
          mincount: 1,
          limit: 1000,
          field: 'countryCode',
        },
      };

      facetPivotDownloads.push('country');
      downloadsQueryParams.facet.id.facet = {
        country: {
          type: 'terms',
          mincount: 1,
          limit: 1000,
          field: 'countryCode',
        },
      };
    } else if (aggregate === 'city') {
      facetPivotViews.push('city');
      viewsQueryParams.facet.id.facet = {
        city: {
          type: 'terms',
          mincount: 1,
          limit: 1000,
          field: 'city',
        },
      };

      facetPivotDownloads.push('city');
      downloadsQueryParams.facet.id.facet = {
        city: {
          type: 'terms',
          mincount: 1,
          limit: 1000,
          field: 'city',
        },
      };
    }

    let periodMonths = [];
    if (startDate != null) {
      const dateRegex =
        /^[0-9]{4}-((0[1-9])|(1[0-2]))-((0[1-9])|([1-2][0-9])|(3[0-1]))$/;
      const startDateMatches = dateRegex.exec(startDate);
      if (
        Array.isArray(startDateMatches) &&
        startDateMatches.length > 0 &&
        startDateMatches[0] === startDate
      ) {
        if (endDate != null) {
          const endDateMatches = dateRegex.exec(endDate);
          endDate =
            Array.isArray(endDateMatches) &&
            endDateMatches.length > 0 &&
            endDateMatches[0] === endDate
              ? endDate
              : new Date().toISOString().split('T')[0];
        } else {
          endDate = new Date().toISOString().split('T')[0];
        }

        // As the statistics will be returned by month, the period should start from the first day of the month
        // from `startDate` and end with the last day of the month `endDate`

        // Set `startDate` to the first day in the month
        const startDateObj = new Date(startDate);
        startDateObj.setDate(1);
        // Convert `start_date` to solr date format
        startDate = `${startDateObj.toISOString().split('T')[0]}T00:00:00Z`;

        // Set `end_date` to the last day in month
        const endDateObj = new Date(endDate);
        endDateObj.setDate(1);
        // Add a month
        endDateObj.setMonth(endDateObj.getMonth() + 1);
        // Go back one day
        endDateObj.setDate(0);

        if (aggregate === 'month') {
          facetPivotViews.push('month');
          facetPivotDownloads.push('month');
          viewsQueryParams.facet.id.facet = {
            month: {
              type: 'range',
              field: 'time',
              start: startDate,
              end: `${endDateObj.toISOString().split('T')[0]}T23:59:59Z`,
              gap: '+1MONTH',
            },
          };
          downloadsQueryParams.facet.id.facet = {
            month: {
              type: 'range',
              field: 'time',
              start: startDate,
              end: `${endDateObj.toISOString().split('T')[0]}T23:59:59Z`,
              gap: '+1MONTH',
            },
          };
          periodMonths = this.getMonthsPeriod(startDateObj, endDateObj);
        }
        viewsQueryParams.filter.push(
          `time:[${startDate} TO ${
            endDateObj.toISOString().split('T')[0]
          }T23:59:59Z]`,
        );
        downloadsQueryParams.filter.push(
          `time:[${startDate} TO ${
            endDateObj.toISOString().split('T')[0]
          }T23:59:59Z]`,
        );
      } else {
        startDate = endDate = null;
      }
    }

    const views = this.querySolr(viewsQueryParams, facetPivotViews, null);
    const downloads = this.querySolr(
      downloadsQueryParams,
      facetPivotDownloads,
      null,
    );

    return await Promise.all([views, downloads]).then((values) => {
      if (values[0].hasOwnProperty('error')) values[0] = [];
      if (values[1].hasOwnProperty('error')) values[1] = [];
      return this.mergeStatisticsData(
        items,
        values[0],
        values[1],
        aggregate,
        periodMonths,
      );
    });
  }

  /**
   * Get date as YYYY-MM between two dates
   */
  getMonthsPeriod(startDateObj: Date, endDateObj: Date) {
    let monthsCount =
      endDateObj.getMonth() -
      startDateObj.getMonth() +
      12 * (endDateObj.getFullYear() - startDateObj.getFullYear());
    const periodMonths = [];
    while (monthsCount >= 0) {
      let month = (startDateObj.getMonth() + 1).toString();
      month = month.length === 1 ? '0' + month : month;
      periodMonths.push(startDateObj.getFullYear() + '-' + month);
      startDateObj.setMonth(startDateObj.getMonth() + 1);
      monthsCount--;
    }
    return periodMonths;
  }

  /**
   * Get statistics from Solr
   */
  async querySolr(
    queryParams = {},
    facetPivot: string[],
    month: string,
  ): Promise<any> {
    const params = JSON.parse(JSON.stringify([queryParams, facetPivot, month]));
    return firstValueFrom(
      this.httpService.post(
        `${process.env.SOLR_SERVER}/statistics/select`,
        queryParams,
      ),
    )
      .then((response) => {
        const result = response?.data;
        if (result?.facets?.id?.buckets) return result.facets.id.buckets;
        else return [];
      })
      .catch((e) => {
        console.log(
          'Error getting Solr statistics => ',
          JSON.stringify(e?.response?.data),
        );
        return {
          error: e?.response?.data?.error,
          params,
        };
      });
  }

  /**
   * Combine items and statistics
   */
  mergeStatisticsData(
    items: any,
    views: any,
    downloads: any,
    aggregate: string,
    periodMonths: string[],
  ) {
    const statistics = items.map((item: any) => {
      let currentViews = null;
      views = views.filter((view: any) => {
        if (view.val === item.uuid) {
          currentViews = view;
        }
        return view.val !== item.uuid;
      });

      let currentDownloads = null;
      downloads = downloads.filter((download: any) => {
        if (download.val === item.uuid) {
          currentDownloads = download;
        }
        return download.val !== item.uuid;
      });

      const countries = {};
      const cities = {};
      const months = {};
      if (
        aggregate === 'country' ||
        aggregate === 'city' ||
        aggregate === 'month'
      ) {
        if (
          currentViews != null &&
          currentViews.hasOwnProperty(aggregate) &&
          currentViews[aggregate]?.buckets
        ) {
          if (aggregate === 'month') {
            currentViews[aggregate].buckets.map((bucket: any) => {
              const monthDateArray = bucket.val.split('-');
              const month = `${monthDateArray[0]}-${monthDateArray[1]}`;
              months[month] = {
                month: month,
                views: bucket.count,
                downloads: 0,
              };
            });
          } else if (aggregate === 'country') {
            currentViews[aggregate].buckets.map((bucket: any) => {
              countries[bucket.val] = {
                country_iso: bucket.val,
                views: bucket.count,
                downloads: 0,
              };
            });
          } else if (aggregate === 'city') {
            currentViews[aggregate].buckets.map((bucket: any) => {
              cities[bucket.val] = {
                city_name: bucket.val,
                views: bucket.count,
                downloads: 0,
              };
            });
          }
        }
        if (
          currentDownloads != null &&
          currentDownloads.hasOwnProperty(aggregate) &&
          currentDownloads[aggregate]?.buckets
        ) {
          if (aggregate === 'month') {
            currentDownloads[aggregate].buckets.map((bucket: any) => {
              const monthDateArray = bucket.val.split('-');
              const month = `${monthDateArray[0]}-${monthDateArray[1]}`;
              if (!months.hasOwnProperty(month)) {
                months[month] = {
                  month: month,
                  views: 0,
                  downloads: bucket.count,
                };
              } else {
                months[month].downloads = bucket.count;
              }
            });
          } else if (aggregate === 'country') {
            currentDownloads[aggregate].buckets.map((bucket: any) => {
              if (!countries.hasOwnProperty(bucket.val)) {
                countries[bucket.val] = {
                  country_iso: bucket.val,
                  views: 0,
                  downloads: bucket.count,
                };
              } else {
                countries[bucket.val].downloads = bucket.count;
              }
            });
          } else if (aggregate === 'city') {
            currentDownloads[aggregate].buckets.map((bucket: any) => {
              if (!cities.hasOwnProperty(bucket.val)) {
                cities[bucket.val] = {
                  city_name: bucket.val,
                  views: 0,
                  downloads: bucket.count,
                };
              } else {
                cities[bucket.val].downloads = bucket.count;
              }
            });
          }
        }
      }
      const data: any = {
        item_id: item.uuid,
        uuid: item.uuid,
        id: item.uuid,
        handle: item.handle,
        title: item.title,
        views: currentViews != null ? currentViews.count : 0,
        downloads: currentDownloads != null ? currentDownloads.count : 0,
        country: [],
        city: [],
        month: [],
      };
      if (aggregate === 'country') {
        data.country = Object.values(countries);
      } else if (aggregate === 'city') {
        data.city = Object.values(cities);
      } else if (aggregate === 'month') {
        data.month = Object.values(months);
      }
      return data;
    });

    return {
      periodMonths,
      statistics,
    };
  }

  /**
   * Export statistics into CSV
   */
  async csvExport(
    items: any,
    startDate: string,
    endDate: string,
    aggregate: string,
    viewsMainKey: string,
    downloadsMainKey: string,
  ): Promise<any> {
    const rows = [];
    let aggregateMonths = false;

    const chunkSize = 100;
    for (let i = 0; i < items.length; i += chunkSize) {
      const chunk = items.slice(i, i + chunkSize);
      console.time(`chunk ${i}`);
      const data = await this.getStatistics(
        chunk,
        startDate,
        endDate,
        aggregate,
        viewsMainKey,
        downloadsMainKey,
      );
      if (rows.length === 0) {
        let row = ['UUID', 'Title', 'Handle', 'Total downloads', 'Total views'];
        if (
          data?.periodMonths &&
          Array.isArray(data.periodMonths) &&
          data.periodMonths.length
        ) {
          row = [
            ...row,
            ...`Downloads ${data.periodMonths.join(',Downloads ')}`.split(','),
          ];
          row = [
            ...row,
            ...`Views ${data.periodMonths.join(',Views ')}`.split(','),
          ];
          aggregateMonths = true;
        }
        rows.push(row.join(','));
      }

      if (data?.statistics && data.statistics.length > 0) {
        for (const statisticsItem of data.statistics) {
          let row = [
            statisticsItem.id,
            `"${JSON.parse(
              JSON.stringify(statisticsItem.title.replace(/"/g, '""')),
            )}"`,
            `${process.env.HANDLE_URL}/${statisticsItem.handle}`,
            statisticsItem.downloads,
            statisticsItem.views,
          ];
          if (aggregateMonths) {
            const monthlyDownloads = [];
            const monthlyViews = [];
            if (
              statisticsItem?.month &&
              Array.isArray(statisticsItem.month) &&
              statisticsItem.month.length > 0
            ) {
              statisticsItem.month.map((month: any) => {
                monthlyDownloads.push(month.downloads);
                monthlyViews.push(month.views);
              });
            } else {
              data.periodMonths.map(() => {
                monthlyDownloads.push(0);
                monthlyViews.push(0);
              });
            }
            row = [...row, ...monthlyDownloads];
            row = [...row, ...monthlyViews];
          }
          rows.push(row.join(','));
        }
      }
      console.timeEnd(`chunk ${i}`);
    }
    return rows.join('\n');
  }
}
