import {Injectable} from '@nestjs/common';
import {DataSource} from 'typeorm';
import {SharedService} from '../shared/shared.service';

@Injectable()
export class CollectionsService {
    constructor(
        private dataSource: DataSource,
        private sharedService: SharedService,
    ) {
    }

    async get(
        uuid: string,
        limit: number,
        page: number,
        startDate: string,
        endDate: string,
        aggregate: string,
    ): Promise<any> {
        aggregate = this.sharedService.validateAggregationParam(aggregate);
        limit = this.sharedService.validateLimitParam(limit);
        page = this.sharedService.validatePageParam(page);
        [startDate, endDate] = this.sharedService.validateDateParam(startDate, endDate, aggregate);

        let total_pages = 0;
        if (uuid == null) {
            const totalQuery = this.dataSource
                .createQueryBuilder()
                .select([
                    'COUNT(collection.uuid)',
                ])
                .from('collection', 'collection');
            const total = await totalQuery.getRawOne();
            if (Number(total.count) > 0)
                total_pages = Math.ceil(Number(total.count) / limit);
        }

        // Get collections
        const collections = this.getCollections(uuid, limit, page);
        // Get statistics shards
        const shards = this.sharedService.getStatisticsShards();

        return await Promise.all([collections, shards])
            .then(async (values) => {
                let data: any = await this.sharedService.getStatistics(values[0], values[1], startDate, endDate, aggregate, process.env.SOLR_VIEWS_KEY_COLLECTION, process.env.SOLR_DOWNLOADS_KEY_COLLECTION);

                if (uuid == null) {
                    data = Object.assign({
                        current_page: page,
                        limit: limit,
                        total_pages: total_pages,
                    }, data);
                }
                return data;
            });
    }

    async getCollections(
        uuid: string,
        limit: number,
        page: number,
    ): Promise<any> {
        const titleMetadataField = await this.sharedService.getTitleMetadataField();
        const titleMetadataFieldId = titleMetadataField?.metadata_field_id;
        const query = this.dataSource
            .createQueryBuilder()
            .select([
                'collection.uuid AS uuid',
                'MAX(handle.handle) AS handle',
                'MAX(metadatavalue.text_value) AS title',
            ])
            .from('collection', 'collection')
            .leftJoin('handle', 'handle', `handle.resource_id = collection.uuid AND handle.resource_type_id = ${Number(process.env.DSPACE_COLLECTION_RESOURCE_TYPE_ID)}`)
            .leftJoin('metadatavalue', 'metadatavalue', 'metadatavalue.dspace_object_id = collection.uuid')
            .groupBy('collection.uuid');

        if (titleMetadataFieldId) {
            query.andWhere('metadatavalue.metadata_field_id = :titleMetadataFieldId', {titleMetadataFieldId});
        }

        if (uuid != null) {
            query.andWhere('collection.uuid = :uuid', {uuid});
        } else if (limit > 0) {
            query
                .limit(limit)
                .offset((page - 1) * limit);
        }
        return await query.execute();
    }

    async csvExport(
        uuid: string,
        startDate: string,
        endDate: string,
        aggregate: string,
    ): Promise<any> {
        [startDate, endDate] = this.sharedService.validateDateParam(startDate, endDate, aggregate);
        const items = await this.getCollections(uuid, null, null);
        return await this.sharedService.csvExport(items, startDate, endDate, aggregate, process.env.SOLR_VIEWS_KEY_COLLECTION, process.env.SOLR_DOWNLOADS_KEY_COLLECTION);
    }
}
