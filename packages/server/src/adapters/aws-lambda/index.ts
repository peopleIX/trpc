/* eslint-disable @typescript-eslint/no-non-null-assertion */
import type {
  APIGatewayProxyEvent,
  APIGatewayProxyEventV2,
  APIGatewayProxyResult,
  APIGatewayProxyStructuredResultV2,
  Context as APIGWContext,
} from 'aws-lambda';
import { TRPCError } from '../..';
import type { AnyRouter, inferRouterContext } from '../../core';
import type { HTTPRequest } from '../../http';
import { resolveHTTPResponse, getBatchStreamFormatter } from '../../http';
import type { HTTPResponse, ResponseChunk } from '../../http/internals/types';
import type {
  APIGatewayEvent,
  APIGatewayResult,
  AWSLambdaOptions,
} from './utils';
import {
  getHTTPMethod,
  getPath,
  isPayloadV1,
  isPayloadV2,
  transformHeaders,
  UNKNOWN_PAYLOAD_FORMAT_VERSION_ERROR_MESSAGE,
} from './utils';
import { Readable } from 'node:stream';

export * from './utils';

function lambdaEventToHTTPRequest(event: APIGatewayEvent): HTTPRequest {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(
    event.queryStringParameters ?? {},
  )) {
    if (typeof value !== 'undefined') {
      query.append(key, value);
    }
  }

  let body: string | null | undefined;
  if (event.body && event.isBase64Encoded) {
    body = Buffer.from(event.body, 'base64').toString('utf8');
  } else {
    body = event.body;
  }

  return {
    method: getHTTPMethod(event),
    query: query,
    headers: event.headers,
    body: body,
  };
}

function tRPCOutputToAPIGatewayOutput<
  TEvent extends APIGatewayEvent,
  TResult extends APIGatewayResult,
>(event: TEvent, response: HTTPResponse): TResult {
  if (isPayloadV1(event)) {
    const resp: APIGatewayProxyResult = {
      statusCode: response.status,
      body: response.body ?? '',
      headers: transformHeaders(response.headers ?? {}),
    };
    return resp as TResult;
  } else if (isPayloadV2(event)) {
    const resp: APIGatewayProxyStructuredResultV2 = {
      statusCode: response.status,
      body: response.body ?? undefined,
      headers: transformHeaders(response.headers ?? {}),
    };
    return resp as TResult;
  } else {
    throw new TRPCError({
      code: 'INTERNAL_SERVER_ERROR',
      message: UNKNOWN_PAYLOAD_FORMAT_VERSION_ERROR_MESSAGE,
    });
  }
}

/** 1:1 mapping of v1 or v2 input events, deduces which is which.
 * @internal
 **/
type inferAPIGWReturn<TType> = TType extends APIGatewayProxyEvent
  ? APIGatewayProxyResult
  : TType extends APIGatewayProxyEventV2
  ? APIGatewayProxyStructuredResultV2
  : never;
export function awsLambdaRequestHandler<
  TRouter extends AnyRouter,
  TEvent extends APIGatewayEvent,
  TResult extends inferAPIGWReturn<TEvent>,
>(
  opts: AWSLambdaOptions<TRouter, TEvent>,
): (event: TEvent, context: APIGWContext) => Promise<TResult> {
  return async (event, context) => {
    const req = lambdaEventToHTTPRequest(event);
    const path = getPath(event);
    const createContext = async function _createContext(): Promise<
      inferRouterContext<TRouter>
    > {
      return await opts.createContext?.({ event, context });
    };

    const response = await resolveHTTPResponse({
      router: opts.router,
      batching: opts.batching,
      responseMeta: opts?.responseMeta,
      createContext,
      req,
      path,
      error: null,
      onError(o) {
        opts?.onError?.({
          ...o,
          req: event,
        });
      },
    });

    return tRPCOutputToAPIGatewayOutput<TEvent, TResult>(event, response);
  };
}

type StreamingAPIGatewayProxyStructuredResultV2 = Omit<APIGatewayProxyStructuredResultV2, 'body'> & {
  body: APIGatewayProxyStructuredResultV2['body'] | Readable;
} | string;

export function awsLambdaStreamingRequestHandler<
  TRouter extends AnyRouter,
  TEvent extends APIGatewayEvent,
>(
  opts: AWSLambdaOptions<TRouter, TEvent>,
): (event: TEvent, context: APIGWContext) => Promise<StreamingAPIGatewayProxyStructuredResultV2> {
  return async (event, context) => {
    const req = lambdaEventToHTTPRequest(event);
    const path = getPath(event);
    const createContext = async function _createContext(): Promise<
      inferRouterContext<TRouter>
    > {
      return await opts.createContext?.({ event, context });
    };

    let resolve: (value: StreamingAPIGatewayProxyStructuredResultV2) => void;
    const promise = new Promise<StreamingAPIGatewayProxyStructuredResultV2>((r) => (resolve = r));

    let isStream = false;
    let stream: Readable;
    let formatter: ReturnType<typeof getBatchStreamFormatter>;

    const unstable_onHead = (head: HTTPResponse, isStreaming: boolean) => {
      if (isStreaming) {
        const headers = transformHeaders(head.headers ?? {}) ?? {};
        headers['Transfer-Encoding'] = 'chunked';
        const vary = headers.Vary;
        headers.Vary = vary ? 'trpc-batch-mode, ' + vary : 'trpc-batch-mode';
        isStream = true;
        formatter = getBatchStreamFormatter();;
        stream = new Readable();
        stream._read = () => { }; // eslint-disable-line @typescript-eslint/no-empty-function -- https://github.com/fastify/fastify/issues/805#issuecomment-369172154
        resolve(
          {
            statusCode: head.status,
            headers,
            body: stream,
          }
        );
      }
    };

    const unstable_onChunk = ([index, string]: ResponseChunk) => {
      if (index === -1) {
        /**
         * Full response, no streaming. This can happen
         * - if the response is an error
         * - if response is empty (HEAD request)
         */
        resolve(string)
      } else {
        stream.push(formatter(index, string));
      }
    };

    resolveHTTPResponse({
      router: opts.router,
      batching: opts.batching,
      responseMeta: opts?.responseMeta,
      createContext,
      req,
      path,
      error: null,
      onError(o) {
        opts?.onError?.({
          ...o,
          req: event,
        });
      },
      unstable_onHead,
      unstable_onChunk,
    })
      .then(() => {
        if (isStream) {
          stream.push(formatter.end());
          stream.push(null); // https://github.com/fastify/fastify/issues/805#issuecomment-369172154
        }
      })
      .catch(() => {
        if (isStream) {
          stream.push(null);
        }
      });

    return promise;
  };
}
