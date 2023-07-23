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

    validateAggregationParam(aggregate) {
        // Allow aggregate by country and city only
        return aggregate === 'country' || aggregate === 'city' ? aggregate : null;
    }

    validateLimitParam(limit) {
        // limit should be between 1 and 100
        limit = Math.abs(limit);
        limit = isNaN(limit) ? 0 : limit;
        limit = limit > 100 ? 100 : limit;
        return limit > 0 ? limit : 100;
    }

    validatePageParam(page) {
        // page should be a positive number
        page = Number(page);
        return page >= 1 ? page : 1;
    }

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

    async getStatistics(
        items: any,
        startDate: string,
        endDate: string,
        aggregate: string,
        solrViewsMainKey: string,
        solrDownloadsMainKey: string,
    ) {
        // Get statistics shards
        const shards = await this.getStatisticsShards();

        // Define common views query params
        const viewsQueryParams = {
            'facet': 'true',
            'facet.mincount': 1,
            'shards': shards,
            'rows': 1,
            'wt': 'json',
            'json.nl': 'map',// return facets as a dict instead of a flat list
            'q': 'type:2',
            'fq': 'isBot:false AND statistics_type:view'
        }
        // Define common downloads query params
        const downloadsQueryParams = {
            'facet': 'true',
            'facet.mincount': 1,
            'shards': shards,
            'rows': 0,
            'wt': 'json',
            'json.nl': 'map',// return facets as a dict instead of a flat list
            'q': 'type:0',
            'fq': 'isBot:false AND statistics_type:view AND bundleName:ORIGINAL'
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
                // Convert `endDate` to solr date format
                endDate = `${endDateObj.toISOString().split('T')[0]}T00:00:00Z`;

                periodMonths = this.getMonthsPeriod(startDateObj, endDateObj);

                viewsQueryParams['facet.range'] = downloadsQueryParams['facet.range'] = 'time';
                viewsQueryParams['facet.range.gap'] = downloadsQueryParams['facet.range.gap'] = '+1MONTH';
                viewsQueryParams['facet.range.start'] = downloadsQueryParams['facet.range.start'] = startDate;
                viewsQueryParams['facet.range.end'] = downloadsQueryParams['facet.range.end'] = endDate;

                viewsQueryParams['fq'] += ` AND time:{${startDate} TO ${endDateObj.toISOString().split('T')[0]}T23:59:59Z}`;
                downloadsQueryParams['fq'] += ` AND time:{${startDate} TO ${endDateObj.toISOString().split('T')[0]}T23:59:59Z}`;
            } else {
                startDate = endDate = null;
            }
        }

        const facetPivotViews = [`${solrViewsMainKey}`];
        const facetPivotDownloads = [`${solrDownloadsMainKey}`];
        if (aggregate === 'country') {
            facetPivotViews.push('countryCode')
            facetPivotDownloads.push('countryCode')
        } else if (aggregate === 'city') {
            facetPivotViews.push('city')
            facetPivotDownloads.push('city')
        }
        viewsQueryParams['facet.pivot'] = facetPivotViews.join(',');
        downloadsQueryParams['facet.pivot'] = facetPivotDownloads.join(',');

        const views = await this.querySolr(aggregate, periodMonths, viewsQueryParams, facetPivotViews);
        const downloads = await this.querySolr(aggregate, periodMonths, downloadsQueryParams, facetPivotDownloads);

        return this.mergeStatisticsData(items, views, downloads, aggregate);
    }

    getStatisticsShards(): Promise<any> {
        /*
         * Enumerate the cores in Solr to determine if statistics have been sharded into
         * yearly shards by DSpace's stats-util or not (for example: statistics-2018).
         *
         * Return the string of shards, which may actually be empty. Solr doesn't
         * seem to mind if the shards query parameter is empty and I haven't seen
         * any negative performance impact so this should be fine.
         */

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

    getMonthsPeriod(startDateObj, endDateObj) {
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

    async querySolr(
        aggregate: string,
        periodMonths: {},
        queryParams: {},
        facetPivot: string[],
    ): Promise<any> {
        return firstValueFrom(this.httpService.get(`${process.env.SOLR_SERVER}/statistics/select`, {
            params: queryParams
        }))
            .then((response) => {
                const result = response?.data?.facet_counts;
                const data = result?.facet_pivot.hasOwnProperty(facetPivot.join(',')) ? result.facet_pivot[facetPivot.join(',')] : null

                const total_by_month = JSON.parse(JSON.stringify(periodMonths));
                const byMonth = result?.facet_ranges?.time?.counts;
                if (typeof byMonth === 'object' && byMonth != null) {
                    for (const month in byMonth) {
                        if (byMonth.hasOwnProperty(month)) {
                            const dateArray = month.split('-');
                            const dateMonth = dateArray[0] + '-' + dateArray[1];
                            if (total_by_month.hasOwnProperty(dateMonth)) {
                                total_by_month[dateMonth] = byMonth[month];
                            }
                        }
                    }
                }

                return {
                    data,
                    total_by_month,
                }
            })
            .catch(e => {
                console.log('Error getting Solr statistics => ', e.response.data)
                return {
                    data: [],
                    total_by_month: []
                };
            });
    }

    mergeStatisticsData(items, views, downloads, aggregate) {
        const statistics = items.map((item) => {
            let currentViews = null;
            views.data = views.data.filter((view) => {
                if (view.value === item.uuid) {
                    currentViews = view;
                }
                return view.value !== item.uuid;
            });

            let currentDownloads = null;
            downloads.data = downloads.data.filter((download) => {
                if (download.value === item.uuid) {
                    currentDownloads = download;
                }
                return download.value !== item.uuid;
            });

            const countries = {};
            const cities = {};
            if (aggregate != null) {
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
                uuid: item.uuid,
                id: item.uuid,
                handle: item.handle,
                title: item.title,
                views: currentViews != null ? currentViews.count : 0,
                downloads: currentDownloads != null ? currentDownloads.count : 0,
                countries: [],
                cities: [],
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
            total_views_by_month: views.total_by_month,
            total_downloads_by_month: downloads.total_by_month,
        };
    }

    async csvExport(
        items: any,
        startDate: string,
        endDate: string,
        viewsMainKey: string,
        downloadsMainKey: string,
    ): Promise<any> {
        const rows = [];
        for (const item of items) {
            const data: any = await this.getStatistics([item], startDate, endDate, null, viewsMainKey, downloadsMainKey);
            if (data?.statistics && data.statistics.length > 0) {
                const statisticsItem = data.statistics[0];
                if (rows.length === 0) {
                    let row = [
                        'UUID',
                        'Title',
                        'Handle',
                        'Total downloads',
                        'Total views',
                    ];
                    if (data?.total_downloads_by_month && typeof data.total_downloads_by_month === 'object' && Object.keys(data.total_downloads_by_month).length > 0) {
                        row = [...row, ...(`Downloads ${Object.keys(data.total_downloads_by_month).join(',Downloads ')}`).split(',')];
                    }
                    if (data?.total_views_by_month && typeof data.total_views_by_month === 'object' && Object.keys(data.total_views_by_month).length > 0) {
                        row = [...row, ...(`Views ${Object.keys(data.total_views_by_month).join(',Views ')}`).split(',')];
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
                if (data?.total_downloads_by_month && typeof data.total_downloads_by_month === 'object' && Object.keys(data.total_downloads_by_month).length > 0) {
                    row = [...row, ...Object.values(data.total_downloads_by_month)];
                }
                if (data?.total_views_by_month && typeof data.total_views_by_month === 'object' && Object.keys(data.total_views_by_month).length > 0) {
                    row = [...row, ...Object.values(data.total_views_by_month)];
                }
                rows.push(row.join(','));
            }
        }
        return rows.join('\n');
    }
}
