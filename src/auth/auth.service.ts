import {Injectable} from '@nestjs/common';

@Injectable()
export class AuthService {
    validateApiKey(apiKey: string) {
        return process.env.API_KEY.trim() !== '' && process.env.API_KEY === apiKey;
    }
}
