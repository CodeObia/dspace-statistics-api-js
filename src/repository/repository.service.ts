import { Injectable } from '@nestjs/common';
import { SharedService } from '../shared/shared.service';

@Injectable()
export class RepositoryService {
  constructor(private sharedService: SharedService) {}

  async get(
    startDate: string,
    endDate: string,
    aggregate: string,
  ): Promise<any> {
    aggregate = this.sharedService.validateAggregationParam(aggregate);
    [startDate, endDate] = this.sharedService.validateDateParam(
      startDate,
      endDate,
      aggregate,
    );

    const data: any = await this.sharedService.getStatistics(
      null,
      startDate,
      endDate,
      aggregate,
      process.env.SOLR_VIEWS_KEY_REPOSITORY,
      process.env.SOLR_DOWNLOADS_KEY_REPOSITORY,
    );
    delete data.periodMonths;

    return data;
  }

  async csvExport(
    startDate: string,
    endDate: string,
    aggregate: string,
  ): Promise<any> {
    [startDate, endDate] = this.sharedService.validateDateParam(
      startDate,
      endDate,
      aggregate,
    );
    return await this.sharedService.csvExport(
      null,
      startDate,
      endDate,
      aggregate,
      process.env.SOLR_VIEWS_KEY_REPOSITORY,
      process.env.SOLR_DOWNLOADS_KEY_REPOSITORY,
    );
  }
}
