import { Module } from '@nestjs/common';
import { SharedService } from './shared.service';
import { HttpModule } from '@nestjs/axios';
import { TypeOrmModule } from '@nestjs/typeorm';

@Module({
  imports: [TypeOrmModule, HttpModule],
  providers: [SharedService],
  exports: [TypeOrmModule, HttpModule, SharedService],
})
export class SharedModule {}
