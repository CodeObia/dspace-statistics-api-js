import {Module} from '@nestjs/common';
import {CollectionsService} from './collections.service';
import {CollectionsController} from './collections.controller';
import {SharedModule} from '../shared/shared.module';

@Module({
    imports: [SharedModule],
    providers: [CollectionsService],
    controllers: [CollectionsController]
})
export class CollectionsModule {
}
