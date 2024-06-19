import {Injectable} from '@nestjs/common';
import {HttpService} from '@nestjs/axios';
import {firstValueFrom} from 'rxjs';
import {DataSource} from 'typeorm';

@Injectable()
export class SharedService {
    constructor(
        private dataSource: DataSource,
        private readonly httpService: HttpService,
    ) {
    }

    /**
     * Allow aggregate by country, city and month only
     */
    validateAggregationParam(aggregate: string) {
        return aggregate === 'country' || aggregate === 'city' || aggregate === 'month' ? aggregate : null;
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
                endDate = `${(startDateObject > today ? today : startDateObject).toISOString().split('T')[0]}`;
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
                endDate = `${(endDateObject > startDateObject ? (startDateObject > today ? today : startDateObject) : (endDateObject > today ? today : endDateObject)).toISOString().split('T')[0]}`;
            }
        }
        return [
            startDate,
            endDate
        ]
    }

    /**
     * Get items title metadata field ID
     * @return string
     */
    getTitleMetadataField() {
        let titleMetadataFieldArray = process.env.DSPACE_TITLE_METADATA_FIELD.split('.');

        // Default title metadata field, dc.title
        if (titleMetadataFieldArray.length < 2) {
            titleMetadataFieldArray = [
                'dc',
                'title'
            ]
        }
        const schema = titleMetadataFieldArray[0];
        const element = titleMetadataFieldArray[1];
        const qualifier = titleMetadataFieldArray.length === 3 ? titleMetadataFieldArray[2] : null;

        const query = this.dataSource
            .createQueryBuilder()
            .select([
                'metadatafieldregistry.metadata_field_id AS metadata_field_id',
            ])
            .from('metadataschemaregistry', 'metadataschemaregistry')
            .innerJoin('metadatafieldregistry', 'metadatafieldregistry', 'metadatafieldregistry.metadata_schema_id = metadataschemaregistry.metadata_schema_id')
            .where('metadataschemaregistry.short_id = :schema', {schema})
            .andWhere('metadatafieldregistry.element = :element', {element});
        if (qualifier != null)
            query.andWhere('metadatafieldregistry.qualifier = :qualifier', {qualifier});
        else
            query.andWhere('metadatafieldregistry.qualifier IS NULL');
        return query.getRawOne();
    }

    /**
     * Get DSpace statistics
     */
    async getStatistics(
        items: any,
        shards: any,
        startDate: string,
        endDate: string,
        aggregate: string,
        solrViewsMainKey: string,
        solrDownloadsMainKey: string,
    ) {
        // Define common views query params
        const viewsQueryParams: any = {
            'facet': 'true',
            'facet.mincount': 1,
            'rows': 0,
            'wt': 'json',
            'json.nl': 'map',// return facets as a dict instead of a flat list
            'q': 'type:2',
            'fq': 'isBot:false AND statistics_type:view'
        }
        // Define common downloads query params
        const downloadsQueryParams: any = {
            'facet': 'true',
            'facet.mincount': 1,
            'shards': shards,
            'rows': 0,
            'wt': 'json',
            'json.nl': 'map',// return facets as a dict instead of a flat list
            'q': 'type:0',
            'fq': 'isBot:false AND statistics_type:view AND bundleName:ORIGINAL'
        }
        if (shards !== '') {
            viewsQueryParams.shards = shards;
            downloadsQueryParams.shards = shards;
        }

        const itemsIds = [];
        items.map((item) => {
            itemsIds.push(item.uuid);
        });
        viewsQueryParams.q += ` AND (${solrViewsMainKey}: ${itemsIds.join(` OR ${solrViewsMainKey}: `)})`;
        downloadsQueryParams.q += ` AND (${solrDownloadsMainKey}: ${itemsIds.join(` OR ${solrDownloadsMainKey}: `)})`;

        let periodMonths = {};
        if (startDate != null) {
            const dateRegex = /^[0-9]{4}-((0[1-9])|(1[0-2]))-((0[1-9])|([1-2][0-9])|(3[0-1]))$/;
            const startDateMatches = dateRegex.exec(startDate);
            if (Array.isArray(startDateMatches) && startDateMatches.length > 0 && startDateMatches[0] === startDate) {
                if (endDate != null) {
                    const endDateMatches = dateRegex.exec(endDate);
                    endDate = Array.isArray(endDateMatches) && endDateMatches.length > 0 && endDateMatches[0] === endDate ? endDate : new Date().toISOString().split('T')[0];
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
                    periodMonths = this.getMonthsPeriod(startDateObj, endDateObj);
                } else {
                    viewsQueryParams['fq'] += ` AND time:{${startDate} TO ${endDateObj.toISOString().split('T')[0]}T23:59:59Z}`;
                    downloadsQueryParams['fq'] += ` AND time:{${startDate} TO ${endDateObj.toISOString().split('T')[0]}T23:59:59Z}`;
                }
            } else {
                startDate = endDate = null;
            }
        }

        const facetPivotViews = [solrViewsMainKey];
        const facetPivotDownloads = [solrDownloadsMainKey];
        if (aggregate === 'country') {
            facetPivotViews.push('countryCode')
            facetPivotDownloads.push('countryCode')
        } else if (aggregate === 'city') {
            facetPivotViews.push('city')
            facetPivotDownloads.push('city')
        }
        viewsQueryParams['facet.pivot'] = facetPivotViews.join(',');
        downloadsQueryParams['facet.pivot'] = facetPivotDownloads.join(',');

        if (aggregate === 'month') {
            const viewsQueryParamsFQ = viewsQueryParams['fq'];
            const downloadsQueryParamsFQ = downloadsQueryParams['fq'];
            let viewsPromises = [];
            let downloadsPromises = [];
            for (const month in periodMonths) {
                if (periodMonths.hasOwnProperty(month)) {
                    const startDateObj = new Date(`${month}-01`);
                    const endDateObj = new Date(`${month}-01`);
                    // Add a month
                    endDateObj.setMonth(endDateObj.getMonth() + 1);
                    // Go back one day
                    endDateObj.setDate(0);

                    viewsQueryParams['fq'] = `${viewsQueryParamsFQ} AND time:{${startDateObj.toISOString().split('T')[0]}T00:00:00Z TO ${endDateObj.toISOString().split('T')[0]}T23:59:59Z}`;
                    downloadsQueryParams['fq'] = `${downloadsQueryParamsFQ} AND time:{${startDateObj.toISOString().split('T')[0]}T00:00:00Z TO ${endDateObj.toISOString().split('T')[0]}T23:59:59Z}`;

                    viewsPromises.push(this.querySolr(viewsQueryParams, facetPivotViews, month));
                    downloadsPromises.push(this.querySolr(downloadsQueryParams, facetPivotDownloads, month));
                }
            }

            let views = [];
            let tries = 0;
            while (viewsPromises.length && tries <= 5) {
                await Promise.all(viewsPromises)
                    .then((values) => {
                        viewsPromises = [];
                        values.map((value) => {
                            if (tries > 0) {
                                console.log(`viewsPromises, try ${tries}`, Object.keys(value));
                            }
                            if (value.hasOwnProperty('error')) {
                                if (value.error?.code === 500) {
                                    viewsPromises.push(this.querySolr(value.params[0], value.params[1], value.params[2], tries));
                                }
                                if (tries === 5) {
                                    console.log(`failed viewsPromises, ${value.params[2]} => `, value.error);
                                }
                            } else {
                                views.push(value);
                            }
                        });
                    });
                tries++;
            }
            const viewsMerged = this.mergeMonthlyStatistics(views, periodMonths);

            let downloads = [];
            tries = 0;
            while (downloadsPromises.length && tries <= 5) {
                await Promise.all(downloadsPromises)
                    .then((values) => {
                        downloadsPromises = [];
                        values.map((value) => {
                            if (tries > 0) {
                                console.log(`downloadsPromises, try ${tries}`, Object.keys(value));
                            }
                            if (value.hasOwnProperty('error')) {
                                if (value.error?.code === 500) {
                                    downloadsPromises.push(this.querySolr(value.params[0], value.params[1], value.params[2], tries));
                                }
                                if (tries === 5) {
                                    console.log(`downloadsPromises, ${value.params[2]} => `, value.error);
                                }
                            } else {
                                downloads.push(value);
                            }
                        });
                    });
                tries++;
            }
            const downloadsMerged = this.mergeMonthlyStatistics(downloads, periodMonths);

            return this.mergeStatisticsData(items, viewsMerged, downloadsMerged, aggregate, periodMonths);
        } else {
            const views = this.querySolr(viewsQueryParams, facetPivotViews, null);
            const downloads = this.querySolr(downloadsQueryParams, facetPivotDownloads, null);

            return await Promise.all([views, downloads])
                .then((values) => {
                    // console.log('values[0] => ', values[0])
                    if (values[0].hasOwnProperty('error'))
                        values[0] = [];
                    if (values[1].hasOwnProperty('error'))
                        values[1] = [];
                    return this.mergeStatisticsData(items, values[0], values[1], aggregate, {});
                });
        }

    }

    /**
     * Merge monthly statistics into one array
     */
    mergeMonthlyStatistics(statisticsArray: any, periodMonths: {}) {
        const data = {};
        for (const statisticsObject of statisticsArray) {
            const month = Object.keys(statisticsObject)[0];
            const statistics = statisticsObject[month];
            for (const statisticItem of statistics) {
                const total_by_month = JSON.parse(JSON.stringify(periodMonths));
                if (!data.hasOwnProperty(statisticItem.value)) {
                    data[statisticItem.value] = {
                        field: 'id',
                        value: statisticItem.value,
                        count: 0,
                        months: total_by_month,
                    }
                }
                data[statisticItem.value].count += statisticItem.count;
                data[statisticItem.value].months[month] = statisticItem.count;
            }
        }
        return Object.values(data);
    }

    /**
     * Enumerate the cores in Solr to determine if statistics have been sharded into
     * yearly shards by DSpace's stats-util or not (for example: statistics-2018).
     *
     * Return the string of shards, which may actually be empty. Solr doesn't
     * seem to mind if the shards query parameter is empty and I haven't seen
     * any negative performance impact so this should be fine.
     *
     * In DSpace7 Solr shards are not supported
     */
    getStatisticsShards(): Promise<any> {
        if (Number(process.env.DSPACE_VERSION) === 7) {
            return new Promise(resolve => resolve(''));
        }
        // Solr status to check active cores
        return firstValueFrom(this.httpService.get(`${process.env.SOLR_SERVER}/admin/cores?action=STATUS&wt=json`))
            .then((response) => {
                const shards = [];
                const cores = response?.data?.status;

                if (typeof cores === 'object' && cores != null) {
                    const regex = /^statistics(-[0-9]{4})?$/;
                    for (const core in cores) {
                        if (cores.hasOwnProperty(core)) {
                            // Check if the core name is "statistics or statistics-YYYY"
                            const matches = regex.exec(core);
                            if (Array.isArray(matches) && matches.length > 0 && matches[0] != null && matches[0] === core) {
                                shards.push(`${process.env.SOLR_SERVER}/${core}`)
                            }
                        }
                    }
                }
                return shards.join(',');
            })
            .catch(e => {
                console.log('Error getting Solr shards => ', e.response.data);
                return '';
            });
    }

    /**
     * Get date as YYYY-MM between two dates
     */
    getMonthsPeriod(startDateObj: Date, endDateObj: Date) {
        let monthsCount = endDateObj.getMonth() - startDateObj.getMonth() + (12 * (endDateObj.getFullYear() - startDateObj.getFullYear()));
        const periodMonths = {};
        while (monthsCount >= 0) {
            let month = (startDateObj.getMonth() + 1).toString();
            month = month.length === 1 ? '0' + month : month;
            periodMonths[startDateObj.getFullYear() + '-' + month] = 0;
            startDateObj.setMonth(startDateObj.getMonth() + 1)
            monthsCount--;
        }
        return periodMonths;
    }

    /**
     * Get statistics from Solr
     */
    async querySolr(
        queryParams: {},
        facetPivot: string[],
        month: string,
        tries: number = 0,
    ): Promise<any> {
        const params = JSON.parse(JSON.stringify([
            queryParams,
            facetPivot,
            month
        ]));
        return firstValueFrom(this.httpService.get(`${process.env.SOLR_SERVER}/statistics/select`, {
            params: queryParams
        }))
            .then((response) => {
                const result = response?.data?.facet_counts;
                if (month != null) {
                    const data: any = {};
                    data[month] = result?.facet_pivot.hasOwnProperty(facetPivot.join(',')) ? result.facet_pivot[facetPivot.join(',')] : [];
                    return data;
                } else {
                    return result?.facet_pivot.hasOwnProperty(facetPivot.join(',')) ? result.facet_pivot[facetPivot.join(',')] : [];
                }
            })
            .catch(e => {
                console.log('Error getting Solr statistics => ', e?.response?.data)
                return {
                    error: e?.response?.data?.error,
                    params
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
        periodMonths: {},
    ) {
        const statistics = items.map((item) => {
            let currentViews = null;
            views = views.filter((view) => {
                if (view.value === item.uuid) {
                    currentViews = view;
                }
                return view.value !== item.uuid;
            });

            let currentDownloads = null;
            downloads = downloads.filter((download) => {
                if (download.value === item.uuid) {
                    currentDownloads = download;
                }
                return download.value !== item.uuid;
            });

            const countries = {};
            const cities = {};
            if (aggregate === 'country' || aggregate === 'city') {
                if (currentViews != null) {
                    currentViews.pivot.map((pivot) => {
                        if (aggregate === 'country') {
                            countries[pivot.value] = {
                                country_iso: pivot.value,
                                views: pivot.count,
                                downloads: 0
                            }
                        } else if (aggregate === 'city') {
                            cities[pivot.value] = {
                                city_name: pivot.value,
                                views: pivot.count,
                                downloads: 0
                            }
                        }
                    });
                }
                if (currentDownloads != null) {
                    currentDownloads.pivot.map((pivot) => {
                        if (aggregate === 'country') {
                            if (!countries.hasOwnProperty(pivot.value)) {
                                countries[pivot.value] = {
                                    country_iso: pivot.value,
                                    views: 0,
                                    downloads: pivot.count,
                                }
                            } else {
                                countries[pivot.value].downloads = pivot.count;
                            }
                        } else if (aggregate === 'city') {
                            if (!countries.hasOwnProperty(pivot.value)) {
                                cities[pivot.value] = {
                                    city_name: pivot.value,
                                    views: 0,
                                    downloads: pivot.count,
                                }
                            } else {
                                cities[pivot.value].downloads = pivot.count;
                            }
                        }
                    });
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
                countries: [],
                cities: [],
                views_by_month: currentViews && currentViews.hasOwnProperty('months') ? currentViews.months : JSON.parse(JSON.stringify(periodMonths)),
                downloads_by_month: currentDownloads && currentDownloads.hasOwnProperty('months') ? currentDownloads.months : JSON.parse(JSON.stringify(periodMonths)),
            };
            if (aggregate === 'country') {
                data.countries = Object.values(countries);
            } else if (aggregate === 'city') {
                data.cities = Object.values(cities);
            }
            return data
        });

        return {
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
        // Get statistics shards
        const shards = await this.getStatisticsShards();

        let promises = [];

        const chunkSize = 100;
        for (let i = 0; i < items.length; i += chunkSize) {
            const chunk = items.slice(i, i + chunkSize);
            console.time(`chunk ${i}`)
            promises.push(this.getStatistics(chunk, shards, startDate, endDate, aggregate, viewsMainKey, downloadsMainKey));
            console.log(`chunk ${i} => `, chunk.length)

            await Promise.all(promises)
                .then(async (values) => {
                    for (const data of values) {
                        if (data?.statistics && data.statistics.length > 0) {
                            for (const statisticsItem of data.statistics) {
                                if (rows.length === 0) {
                                    let row = [
                                        'UUID',
                                        'Title',
                                        'Handle',
                                        'Total downloads',
                                        'Total views',
                                    ];
                                    if (statisticsItem?.downloads_by_month && typeof statisticsItem.downloads_by_month === 'object' && Object.keys(statisticsItem.downloads_by_month).length > 0) {
                                        row = [...row, ...(`Downloads ${Object.keys(statisticsItem.downloads_by_month).join(',Downloads ')}`).split(',')];
                                    }
                                    if (statisticsItem?.views_by_month && typeof statisticsItem.views_by_month === 'object' && Object.keys(statisticsItem.views_by_month).length > 0) {
                                        row = [...row, ...(`Views ${Object.keys(statisticsItem.views_by_month).join(',Views ')}`).split(',')];
                                    }
                                    rows.push(row.join(','));
                                }

                                let row = [
                                    statisticsItem.id,
                                    `"${JSON.parse(JSON.stringify(statisticsItem.title.replace(/"/g, '""')))}"`,
                                    `${process.env.HANDLE_URL}/${statisticsItem.handle}`,
                                    statisticsItem.downloads,
                                    statisticsItem.views,
                                ]
                                if (statisticsItem?.downloads_by_month && typeof statisticsItem.downloads_by_month === 'object' && Object.keys(statisticsItem.downloads_by_month).length > 0) {
                                    row = [...row, ...Object.values(statisticsItem.downloads_by_month)];
                                }
                                if (statisticsItem?.views_by_month && typeof statisticsItem.views_by_month === 'object' && Object.keys(statisticsItem.views_by_month).length > 0) {
                                    row = [...row, ...Object.values(statisticsItem.views_by_month)];
                                }
                                rows.push(row.join(','));
                            }
                        }
                    }
                })
                .catch(e => console.log(e));
            console.timeEnd(`chunk ${i}`)
            promises = [];
        }
        return rows.join('\n');
    }
}
