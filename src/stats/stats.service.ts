import { Injectable } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { StatsRequest, FacetTypes, FacetObject } from './stats-common.dto';

@Injectable()
export class StatsService {
  constructor(private readonly httpService: HttpService) {}

  async get(stats: StatsRequest) {
    return {
      ...(stats?.facets ? await this.getStats(stats.facets) : {}),
      ...(stats?.views || stats?.downloads
        ? await this.getStatistics(stats.views, stats.downloads)
        : {}),
    };
  }

  async getStats(facets: FacetObject) {
    const requestBody = {
      query:
        'search.resourcetype:Item AND withdrawn:false AND discoverable:true',
      limit: 0,
      facet: {},
    };

    for (const facetName in facets) {
      if (facets.hasOwnProperty(facetName)) {
        if (facets[facetName].type === FacetTypes.aggregated) {
          requestBody.facet[facetName] = {
            type: 'terms',
            field: facets[facetName].field,
            limit: facets[facetName]?.limit ? facets[facetName].limit : 10,
          };
        } else if (facets[facetName].type === FacetTypes.total_unique) {
          requestBody.facet[facetName] = `unique(${facets[facetName].field})`;
        }
      }
    }

    return (await this.querySolr('search', requestBody))?.facets;
  }

  async getStatistics(views: boolean, downloads: boolean) {
    const response = {
      views: 0,
      downloads: 0,
    };
    if (views) {
      // Define common views query params
      const viewsQueryParams: any = {
        limit: 0,
        query: 'type:2',
        filter: ['-isBot:true', 'statistics_type:view'],
      };

      response.views = (
        await this.querySolr('statistics', viewsQueryParams)
      )?.response?.numFound;
    }
    if (downloads) {
      // Define common downloads query params
      const downloadsQueryParams: any = {
        limit: 0,
        query: 'type:0',
        filter: ['-isBot:true', 'statistics_type:view', 'bundleName:ORIGINAL'],
      };

      response.downloads = (
        await this.querySolr('statistics', downloadsQueryParams)
      )?.response?.numFound;
    }

    return response;
  }

  async querySolr(core: string, body: any) {
    return firstValueFrom(
      this.httpService.post(`${process.env.SOLR_SERVER}/${core}/select`, body),
    )
      .then((response) => {
        return response?.data;
      })
      .catch((e) => {
        console.log(
          'Error getting Solr statistics => ',
          JSON.stringify(e?.response?.data),
        );
        return {
          error: e?.response?.data?.error,
        };
      });
  }
}
