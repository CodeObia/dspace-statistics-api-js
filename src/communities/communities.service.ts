import {Injectable} from '@nestjs/common';
import {DataSource} from 'typeorm';
import {SharedService} from '../shared/shared.service';

@Injectable()
export class CommunitiesService {
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

        let total_pages = 0;
        if (uuid == null) {
            const totalQuery = this.dataSource
                .createQueryBuilder()
                .select([
                    'COUNT(community.uuid)',
                ])
                .from('community', 'community');
            const total = await totalQuery.getRawOne();
            if (Number(total.count) > 0)
                total_pages = Math.ceil(Number(total.count) / limit);
        }

        // Get communities
        const communities = this.getCommunities(uuid, limit, page);
        // Get statistics shards
        const shards = this.sharedService.getStatisticsShards();

        return await Promise.all([communities, shards])
            .then(async (values) => {
                let data: any = await this.sharedService.getStatistics(values[0], values[1], startDate, endDate, aggregate, process.env.SOLR_VIEWS_KEY_COMMUNITY, process.env.SOLR_DOWNLOADS_KEY_COMMUNITY);

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

    async getCommunities(
        uuid: string,
        limit: number,
        page: number,
    ): Promise<any> {
        const titleMetadataField = await this.sharedService.getTitleMetadataField();
        const titleMetadataFieldId = titleMetadataField?.metadata_field_id;
        const query = this.dataSource
            .createQueryBuilder()
            .select([
                'community.uuid AS uuid',
                'MAX(handle.handle) AS handle',
                'MAX(metadatavalue.text_value) AS title',
            ])
            .from('community', 'community')
            .leftJoin('handle', 'handle', `handle.resource_id = community.uuid AND handle.resource_type_id = ${Number(process.env.DSPACE_COMMUNITY_RESOURCE_TYPE_ID)}`)
            .leftJoin('metadatavalue', 'metadatavalue', 'metadatavalue.dspace_object_id = community.uuid')
            .groupBy('community.uuid');

        if (titleMetadataFieldId) {
            query.andWhere('metadatavalue.metadata_field_id = :titleMetadataFieldId', {titleMetadataFieldId});
        }

        if (uuid != null) {
            query.andWhere('community.uuid = :uuid', {uuid});
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
    ): Promise<any> {
        const items = await this.getCommunities(uuid, null, null);
        return await this.sharedService.csvExport(items, startDate, endDate, process.env.SOLR_VIEWS_KEY_COMMUNITY, process.env.SOLR_DOWNLOADS_KEY_COMMUNITY);
    }
}
