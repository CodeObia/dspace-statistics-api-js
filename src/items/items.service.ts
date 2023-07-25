import {Injectable} from '@nestjs/common';
import {DataSource} from 'typeorm';
import {SharedService} from '../shared/shared.service';

@Injectable()
export class ItemsService {
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
                    'COUNT(item.uuid)',
                ])
                .from('item', 'item')
                .where('item.in_archive = TRUE AND item.withdrawn = FALSE');
            const total = await totalQuery.getRawOne();
            if (Number(total.count) > 0)
                total_pages = Math.ceil(Number(total.count) / limit);
        }

        // Get items
        const items = this.getItems(uuid, limit, page);
        // Get statistics shards
        const shards = this.sharedService.getStatisticsShards();

        return await Promise.all([items, shards])
            .then(async (values) => {
                let data: any = await this.sharedService.getStatistics(values[0], values[1], startDate, endDate, aggregate, process.env.SOLR_VIEWS_KEY_ITEM, process.env.SOLR_DOWNLOADS_KEY_ITEM);

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

    async getItems(
        uuid: string,
        limit: number,
        page: number,
    ): Promise<any> {
        const titleMetadataField = await this.sharedService.getTitleMetadataField();
        const titleMetadataFieldId = titleMetadataField?.metadata_field_id;
        const query = this.dataSource
            .createQueryBuilder()
            .select([
                'item.uuid AS uuid',
                'MAX(handle.handle) AS handle',
                'MAX(metadatavalue.text_value) AS title',
            ])
            .from('item', 'item')
            .leftJoin('handle', 'handle', `handle.resource_id = item.uuid AND handle.resource_type_id = ${Number(process.env.DSPACE_ITEM_RESOURCE_TYPE_ID)}`)
            .leftJoin('metadatavalue', 'metadatavalue', 'metadatavalue.dspace_object_id = item.uuid')
            .where('item.in_archive = TRUE AND item.withdrawn = FALSE')
            .groupBy('item.uuid');

        if (titleMetadataFieldId) {
            query.andWhere('metadatavalue.metadata_field_id = :titleMetadataFieldId', {titleMetadataFieldId});
        }

        if (uuid != null) {
            query.andWhere('item.uuid = :uuid', {uuid});
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
        const items = await this.getItems(uuid, null, null);
        return await this.sharedService.csvExport(items, startDate, endDate, aggregate, process.env.SOLR_VIEWS_KEY_ITEM, process.env.SOLR_DOWNLOADS_KEY_ITEM);
    }
}
