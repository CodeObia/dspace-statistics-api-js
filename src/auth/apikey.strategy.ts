import {HeaderAPIKeyStrategy} from 'passport-headerapikey';
import {PassportStrategy} from '@nestjs/passport';
import {Injectable, UnauthorizedException} from '@nestjs/common';
import {AuthService} from './auth.service';

@Injectable()
export class ApikeyStrategy extends PassportStrategy(HeaderAPIKeyStrategy, 'api-key') {
    constructor(private readonly authService: AuthService) {
        super({header: 'Authorization', prefix: 'Bearer '}, true, async (apiKey, done) => {
            if (this.authService.validateApiKey(apiKey)) {
                done(null, true);
            } else {
                done(new UnauthorizedException(), null);
            }
        });
    }
}