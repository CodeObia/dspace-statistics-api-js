import {Module} from '@nestjs/common';
import {CommunitiesService} from './communities.service';
import {CommunitiesController} from './communities.controller';
import {SharedModule} from '../shared/shared.module';

@Module({
    imports: [SharedModule],
    providers: [CommunitiesService],
    controllers: [CommunitiesController]
})
export class CommunitiesModule {
}
