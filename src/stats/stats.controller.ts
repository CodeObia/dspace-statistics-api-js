import { Body, Controller, Post } from '@nestjs/common';
import { StatsService } from './stats.service';
import { ApiBody, ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { StatsRequest } from './stats-common.dto';

@Controller('stats')
export class StatsController {
  constructor(private readonly statsService: StatsService) {}

  /**
   * Get stats
   */
  @ApiTags('Stats')
  @ApiOperation({ summary: 'Get stats' })
  @ApiBody({ type: StatsRequest })
  @ApiResponse({
    status: 200,
  })
  @Post()
  async findAll(@Body() stats: StatsRequest) {
    return await this.statsService.get(stats);
  }
}
