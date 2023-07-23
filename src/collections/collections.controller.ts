import {Controller, Get, Param, Query, Response, UseGuards} from '@nestjs/common';
import {CollectionsService} from './collections.service';
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

@Controller('collections')
export class CollectionsController {
    constructor(private readonly collectionsService: CollectionsService) {
    }

    /**
     * Get statistics for all Collections
     */
    @ApiTags('Collections')
    @ApiOperation({
        summary: 'Get statistics for all Collections',
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
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if provided the results will be aggregated by month',
    })
    @ApiQuery({
        name: 'end_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if not provided it will default to the current month',
    })
    @ApiQuery({
        name: 'aggregate',
        enum: ['country', 'city'],
        required: false,
        description: 'Collections statistics disaggregated by country|city',
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
        return await this.collectionsService.get(null, limit, page, startDate, endDate, aggregate);
    }

    /**
     * Export statistics for all Collections
     */
    @UseGuards(ApikeyAuthGuard)
    @ApiBearerAuth()
    @ApiTags('Collections')
    @ApiOperation({
        summary: 'Export statistics for all Collections as CSV',
    })
    @ApiQuery({
        name: 'start_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if provided the results will be aggregated by month',
    })
    @ApiQuery({
        name: 'end_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if not provided it will default to the current month',
    })
    @Get('/csv')
    async csvexportAll(
        @Query('start_date') startDate: string = null,
        @Query('end_date') endDate: string = new Date().toISOString().split('T')[0],
        @Response() res: any,
    ) {
        res.set({
            'Content-Type': 'application/octet-stream; charset=utf8',
            'Content-Disposition': `attachment; filename="DSpace-Collections-statistics-${new Date().toISOString()}.csv"`
        });
        const stream = new Readable();
        stream.push(await this.collectionsService.csvExport(null, startDate, endDate));
        stream.setEncoding('utf8')
        stream.push(null);
        stream.pipe(res);
    }

    /**
     * Get statistics for specific Collection
     */
    @ApiTags('Collections')
    @ApiOperation({
        summary: 'Get statistics for specific Collection',
    })
    @ApiParam({
        name: 'uuid',
        required: true,
        description: 'Collection UUID',
    })
    @ApiQuery({
        name: 'start_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if provided the results will be aggregated by month',
    })
    @ApiQuery({
        name: 'end_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if not provided it will default to the current month',
    })
    @ApiQuery({
        name: 'aggregate',
        enum: ['country', 'city'],
        required: false,
        description: 'Collections statistics disaggregated by country|city',
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
        return await this.collectionsService.get(uuid, null, null, startDate, endDate, aggregate);
    }

    /**
     * Export statistics for specific Collection
     */
    @UseGuards(ApikeyAuthGuard)
    @ApiBearerAuth()
    @ApiTags('Collections')
    @ApiOperation({
        summary: 'Export statistics for specific Collection as CSV',
    })
    @ApiParam({
        name: 'uuid',
        required: true,
        description: 'Collection UUID',
    })
    @ApiQuery({
        name: 'start_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if provided the results will be aggregated by month',
    })
    @ApiQuery({
        name: 'end_date',
        required: false,
        description: 'Date (ISO 8601) — Start date for statistics tally by months (Ignores days), if not provided it will default to the current month',
    })
    @Get(':uuid/csv')
    async csvexport(
        @Param('uuid') uuid: string = null,
        @Query('start_date') startDate: string = null,
        @Query('end_date') endDate: string = new Date().toISOString().split('T')[0],
        @Response() res: any,
    ) {
        res.set({
            'Content-Type': 'application/octet-stream; charset=utf8',
            'Content-Disposition': `attachment; filename="DSpace-Collections-statistics-${new Date().toISOString()}.csv"`
        });
        const stream = new Readable();
        stream.push(await this.collectionsService.csvExport(uuid, startDate, endDate));
        stream.setEncoding('utf8')
        stream.push(null);
        stream.pipe(res);
    }
}
