import {ApiProperty} from '@nestjs/swagger'

class CountriesDisaggregation {
    @ApiProperty({description: 'ISO 3166-1 alpha-2 code', default: 'ISO 3166-1 alpha-2 code'})
    country_iso: string;
    @ApiProperty({description: 'Total number of views'})
    views: number;
    @ApiProperty({description: 'Total number of downloads'})
    downloads: number;
}

class CitiesDisaggregation {
    @ApiProperty({description: 'City name', default: 'City name'})
    country_iso: string;
    @ApiProperty({description: 'Total number of views'})
    views: number;
    @ApiProperty({description: 'Total number of downloads'})
    downloads: number;
}

class MonthsDisaggregation {
    @ApiProperty({description: 'Year-month'})
    'Year-Month': number;
}

class StatisticsObject {
    @ApiProperty({description: 'UUID', default: 'UUID'})
    id: string;
    @ApiProperty({description: 'Handle URI', default: 'Handle URI'})
    handle: string;
    @ApiProperty({description: 'Title', default: 'Title'})
    title: string;
    @ApiProperty({description: 'Total number of views'})
    views: number;
    @ApiProperty({description: 'Total number of downloads'})
    downloads: number;
    @ApiProperty({type: [CountriesDisaggregation], description: 'Statistics aggregated by country'})
    countries: CountriesDisaggregation;
    @ApiProperty({type: [CitiesDisaggregation], description: 'Statistics aggregated by city'})
    cities: CitiesDisaggregation;
}

export class SingleResultStatistics {
    @ApiProperty({description: 'Current page'})
    current_page: number;
    @ApiProperty({description: 'Results per page'})
    limit: number;
    @ApiProperty({description: 'Number of pages'})
    total_pages: number;
    @ApiProperty({type: [StatisticsObject], description: 'Statistics'})
    statistics: StatisticsObject;
    @ApiProperty({type: MonthsDisaggregation, description: 'Total number of views aggregated by month'})
    total_views_by_month: MonthsDisaggregation;
    @ApiProperty({type: MonthsDisaggregation, description: 'Total number of downloads aggregated by month'})
    total_downloads_by_month: MonthsDisaggregation;
}

export class MultipleResultsStatistics {
    @ApiProperty({type: StatisticsObject, description: 'Statistics'})
    statistics: StatisticsObject;
    @ApiProperty({type: MonthsDisaggregation, description: 'Total number of views aggregated by month'})
    total_views_by_month: MonthsDisaggregation;
    @ApiProperty({type: MonthsDisaggregation, description: 'Total number of downloads aggregated by month'})
    total_downloads_by_month: MonthsDisaggregation;
}