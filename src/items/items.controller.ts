import {Controller, Get, Param, Query, Response, UseGuards} from '@nestjs/common';
import {ItemsService} from './items.service';
import {
    ApiBearerAuth,
    ApiTags,
    ApiQuery,
    ApiParam,
    ApiResponse,
    getSchemaPath,
    ApiExtraModels,
    ApiOperation
} from '@nestjs/swagger';
import {Readable} from 'stream';
import {ApikeyAuthGuard} from '../auth/apikey-auth.guard';
import {MultipleResultsStatistics, SingleResultStatistics} from '../shared/statistics-common.dto'

@Controller('items')
export class ItemsController {
    constructor(private readonly itemService: ItemsService) {
    }

    /**
     * Get statistics for all Items
     */
    @ApiTags('Items')
    @ApiOperation({
        summary: 'Get statistics for all Items',
    })
    @ApiQuery({
        name: 'limit',
        required: false,
        description: 'Integer between 1 and 100 - Number of results per page',
    })
    @ApiQuery({
        name: 'page',
        required: false,
        description: 'Integer greater than or equal to 1 - Page of results to start on',
    })
    @ApiQuery({
        name: 'start_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days)',
    })
    @ApiQuery({
        name: 'end_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if not provided it will default to the current month',
    })
    @ApiQuery({
        name: 'aggregate',
        enum: ['country', 'city', 'month'],
        required: false,
        description: 'Items statistics disaggregated by country|city|month<br><ul>Selecting aggregate by month is limited to 12 months:<li>Start and End date are not provided: The last 12 months starting from the current month</li><li>Start date is not provided: The last 12 months starting from the End date</li><li>End date is not provided: The next 12 months starting from the Start date or current month</li><li>Start and End date are provided: If the period is exceeding 12 months it will set the End date to 12 months from the Start date</li><b>NOTE: The start_date and date_date parameters will be modified according to the cases above if the month aggregation is selected</b></ul>',
    })
    @ApiExtraModels(MultipleResultsStatistics)
    @ApiResponse({
        status: 200,
        schema: {
            $ref: getSchemaPath(MultipleResultsStatistics),
        },
    })
    @Get()
    async findAll(
        @Query('limit') limit: number = 100,
        @Query('page') page: number = 0,
        @Query('start_date') startDate: string = null,
        @Query('end_date') endDate: string = new Date().toISOString().split('T')[0],
        @Query('aggregate') aggregate: string,
    ) {
        return await this.itemService.get(null, limit, page, startDate, endDate, aggregate);
    }

    /**
     * Export statistics for all Items
     */
    @UseGuards(ApikeyAuthGuard)
    @ApiBearerAuth()
    @ApiTags('Items')
    @ApiOperation({
        summary: 'Export statistics for all Items as CSV',
    })
    @ApiQuery({
        name: 'start_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days)',
    })
    @ApiQuery({
        name: 'end_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if not provided it will default to the current month',
    })
    @ApiQuery({
        name: 'aggregate',
        enum: ['month'],
        required: false,
        description: 'Items statistics disaggregated by month<br><ul>Selecting aggregate by month is limited to 12 months:<li>Start and End date are not provided: The last 12 months starting from the current month</li><li>Start date is not provided: The last 12 months starting from the End date</li><li>End date is not provided: The next 12 months starting from the Start date or current month</li><li>Start and End date are provided: If the period is exceeding 12 months it will set the End date to 12 months from the Start date</li><b>NOTE: The start_date and date_date parameters will be modified according to the cases above if the month aggregation is selected</b></ul>',
    })
    @Get('/csv')
    async csvexportAll(
        @Query('start_date') startDate: string = null,
        @Query('end_date') endDate: string = new Date().toISOString().split('T')[0],
        @Query('aggregate') aggregate: string,
        @Response() res: any,
    ) {
        aggregate = aggregate === 'month' ? aggregate : null;
        res.set({
            'Content-Type': 'application/octet-stream; charset=utf8',
            'Content-Disposition': `attachment; filename="DSpace-Items-statistics-${new Date().toISOString()}.csv"`
        });
        const stream = new Readable();
        console.time('csvexportAll')
        stream.push(await this.itemService.csvExport(null, startDate, endDate, aggregate));
        console.timeEnd('csvexportAll')
        stream.setEncoding('utf8')
        stream.push(null);
        stream.pipe(res);
    }

    /**
     * Get statistics for specific Item
     */
    @ApiTags('Items')
    @ApiOperation({
        summary: 'Get statistics for specific Item',
    })
    @ApiParam({
        name: 'uuid',
        required: true,
        description: 'Item UUID',
    })
    @ApiQuery({
        name: 'start_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days)',
    })
    @ApiQuery({
        name: 'end_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if not provided it will default to the current month',
    })
    @ApiQuery({
        name: 'aggregate',
        enum: ['country', 'city', 'month'],
        required: false,
        description: 'Item statistics disaggregated by country|city|month<br><ul>Selecting aggregate by month is limited to 12 months:<li>Start and End date are not provided: The last 12 months starting from the current month</li><li>Start date is not provided: The last 12 months starting from the End date</li><li>End date is not provided: The next 12 months starting from the Start date or current month</li><li>Start and End date are provided: If the period is exceeding 12 months it will set the End date to 12 months from the Start date</li><b>NOTE: The start_date and date_date parameters will be modified according to the cases above if the month aggregation is selected</b></ul>',
    })
    @ApiExtraModels(SingleResultStatistics)
    @ApiResponse({
        status: 200,
        schema: {
            $ref: getSchemaPath(SingleResultStatistics),
        },
    })
    @Get(':uuid')
    async findOne(
        @Param('uuid') uuid: string = null,
        @Query('start_date') startDate: string = null,
        @Query('end_date') endDate: string = new Date().toISOString().split('T')[0],
        @Query('aggregate') aggregate: string,
    ) {
        return await this.itemService.get(uuid, null, null, startDate, endDate, aggregate);
    }

    /**
     * Export statistics for specific Item
     */
    @UseGuards(ApikeyAuthGuard)
    @ApiBearerAuth()
    @ApiTags('Items')
    @ApiOperation({
        summary: 'Export statistics for specific Item as CSV',
    })
    @ApiParam({
        name: 'uuid',
        required: true,
        description: 'Item UUID',
    })
    @ApiQuery({
        name: 'start_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days)',
    })
    @ApiQuery({
        name: 'end_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if not provided it will default to the current month',
    })
    @ApiQuery({
        name: 'aggregate',
        enum: ['month'],
        required: false,
        description: 'Item statistics disaggregated by month<br><ul>Selecting aggregate by month is limited to 12 months:<li>Start and End date are not provided: The last 12 months starting from the current month</li><li>Start date is not provided: The last 12 months starting from the End date</li><li>End date is not provided: The next 12 months starting from the Start date or current month</li><li>Start and End date are provided: If the period is exceeding 12 months it will set the End date to 12 months from the Start date</li><b>NOTE: The start_date and date_date parameters will be modified according to the cases above if the month aggregation is selected</b></ul>',
    })
    @Get(':uuid/csv')
    async csvexport(
        @Param('uuid') uuid: string = null,
        @Query('start_date') startDate: string = null,
        @Query('end_date') endDate: string = new Date().toISOString().split('T')[0],
        @Query('aggregate') aggregate: string,
        @Response() res: any,
    ) {
        aggregate = aggregate === 'month' ? aggregate : null;
        res.set({
            'Content-Type': 'application/octet-stream; charset=utf8',
            'Content-Disposition': `attachment; filename="DSpace-Items-statistics-${new Date().toISOString()}.csv"`
        });
        const stream = new Readable();
        stream.push(await this.itemService.csvExport(uuid, startDate, endDate, aggregate));
        stream.setEncoding('utf8')
        stream.push(null);
        stream.pipe(res);
    }
}
