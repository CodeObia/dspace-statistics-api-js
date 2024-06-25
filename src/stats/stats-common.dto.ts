import { ApiProperty } from '@nestjs/swagger';

export enum FacetTypes {
  aggregated = 'aggregated',
  total_unique = 'total_unique',
}

class Facet {
  @ApiProperty({ description: 'The field name to facet over' })
  field: string;

  @ApiProperty({
    description: 'Return inputs aggregated or just number of unique inputs',
  })
  type: FacetTypes;

  @ApiProperty({
    description:
      'Limits the number of buckets returned. Defaults to 10. Works only when "aggregated" is selected as type "type"',
  })
  limit: number;
}

export class FacetObject {
  [key: string]: Facet;
}

export class StatsRequest {
  @ApiProperty({ description: 'Facets' })
  facets: FacetObject;
  @ApiProperty({ description: 'Get downloads' })
  downloads: boolean;
  @ApiProperty({ description: 'Get views' })
  views: boolean;
}
