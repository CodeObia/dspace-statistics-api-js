import { Module } from '@nestjs/common';
import { AuthService } from './auth.service';
import { PassportModule } from '@nestjs/passport';
import { ApikeyStrategy } from './apikey.strategy';

@Module({
  imports: [PassportModule],
  providers: [AuthService, ApikeyStrategy],
  exports: [AuthService],
})
export class AuthModule {}
