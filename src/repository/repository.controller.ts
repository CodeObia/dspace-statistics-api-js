import { Controller, Get, Query, Response, UseGuards } from '@nestjs/common';
import { RepositoryService } from './repository.service';
import {
  ApiBearerAuth,
  ApiTags,
  ApiQuery,
  ApiResponse,
  getSchemaPath,
  ApiExtraModels,
  ApiOperation,
} from '@nestjs/swagger';
import { Readable } from 'stream';
import { ApikeyAuthGuard } from '../auth/apikey-auth.guard';
import { SingleResultStatistics } from '../shared/statistics-common.dto';

@Controller('repository')
export class RepositoryController {
  constructor(private readonly repositoryService: RepositoryService) {}

  /**
   * Get statistics for repository
   */
  @ApiTags('Repository')
  @ApiOperation({
    summary: 'Get statistics for repository',
  })
  @ApiQuery({
    name: 'start_date',
    required: false,
    description:
      'Date (ISO 8601) — Start date for statistics tally by months (Ignores days)',
  })
  @ApiQuery({
    name: 'end_date',
    required: false,
    description:
      'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if not provided it will default to the current month',
  })
  @ApiQuery({
    name: 'aggregate',
    enum: ['country', 'city', 'month'],
    required: false,
    description:
      'Repository statistics disaggregated by country|city|month<br><ul>Selecting aggregate by month is limited to 12 months:<li>Start and End date are not provided: The last 12 months starting from the current month</li><li>Start date is not provided: The last 12 months starting from the End date</li><li>End date is not provided: The next 12 months starting from the Start date or current month</li><li>Start and End date are provided: If the period is exceeding 12 months it will set the End date to 12 months from the Start date</li><b>NOTE: The start_date and date_date parameters will be modified according to the cases above if the month aggregation is selected</b></ul>',
  })
  @ApiExtraModels(SingleResultStatistics)
  @ApiResponse({
    status: 200,
    schema: {
      $ref: getSchemaPath(SingleResultStatistics),
    },
  })
  @Get(['', '*'])
  async findOne(
    @Query('start_date') startDate: string = null,
    @Query('end_date') endDate: string = new Date().toISOString().split('T')[0],
    @Query('aggregate') aggregate: string,
  ) {
    return await this.repositoryService.get(startDate, endDate, aggregate);
  }

  /**
   * Export statistics for repository
   */
  @UseGuards(ApikeyAuthGuard)
  @ApiBearerAuth()
  @ApiTags('Repository')
  @ApiOperation({
    summary: 'Export statistics for Repository as CSV',
  })
  @ApiQuery({
    name: 'start_date',
    required: false,
    description:
      'Date (ISO 8601) — Start date for statistics tally by months (Ignores days)',
  })
  @ApiQuery({
    name: 'end_date',
    required: false,
    description:
      'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if not provided it will default to the current month',
  })
  @ApiQuery({
    name: 'aggregate',
    enum: ['month'],
    required: false,
    description:
      'Repository statistics disaggregated by month<br><ul>Selecting aggregate by month is limited to 12 months:<li>Start and End date are not provided: The last 12 months starting from the current month</li><li>Start date is not provided: The last 12 months starting from the End date</li><li>End date is not provided: The next 12 months starting from the Start date or current month</li><li>Start and End date are provided: If the period is exceeding 12 months it will set the End date to 12 months from the Start date</li><b>NOTE: The start_date and date_date parameters will be modified according to the cases above if the month aggregation is selected</b></ul>',
  })
  @Get('csv')
  async csvexport(
    @Query('start_date') startDate: string = null,
    @Query('end_date') endDate: string = new Date().toISOString().split('T')[0],
    @Query('aggregate') aggregate: string,
    @Response() res: any,
  ) {
    aggregate = aggregate === 'month' ? aggregate : null;
    res.set({
      'Content-Type': 'application/octet-stream; charset=utf8',
      'Content-Disposition': `attachment; filename="DSpace-Items-statistics-${new Date().toISOString()}.csv"`,
    });
    const stream = new Readable();
    stream.push(
      await this.repositoryService.csvExport(startDate, endDate, aggregate),
    );
    stream.setEncoding('utf8');
    stream.push(null);
    stream.pipe(res);
  }
}
