import { Module } from '@nestjs/common';
import { StatsController } from './stats.controller';
import { StatsService } from './stats.service';
import { HttpModule } from '@nestjs/axios';
import { SharedService } from '../shared/shared.service';

@Module({
  controllers: [StatsController],
  providers: [StatsService, SharedService],
  imports: [HttpModule],
})
export class StatsModule {}
