import path from 'path';
import * as grpc from '@grpc/grpc-js';
import type { ClientDuplexStream, ServiceError } from '@grpc/grpc-js';
import * as loader from '@grpc/proto-loader';
import express, { Request, Response, NextFunction } from 'express';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const BROKER_ADDR = process.env.BROKER_ADDR ?? 'localhost:50051';
const HTTP_PORT   = Number(process.env.HTTP_PORT ?? 3001);
const DEFAULT_Q   = process.env.QUEUE ?? 'orders';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
interface LogFields {
  [key: string]: string | number | boolean | undefined;
}

interface OrderEvent {
  order_id:   string;
  product:    string;
  quantity:   number;
  status:     string;
  created_at: string;
  [key: string]: unknown;
}

interface GrpcHeader {
  key:   string;
  value: string;
}

interface PublishRequest {
  queue:   string;
  payload: Buffer;
  headers: GrpcHeader[];
}

interface PublishResponse {
  message_id: string;
}

interface BrokerMessage {
  id:          string;
  queue:       string;
  payload:     Buffer;
  retry_count: number;
}

interface SubscribeEvent {
  message?: BrokerMessage;
}

interface ConsumerMessage {
  payload:    string;
  subscribe?: { queue: string; prefetch_limit: number };
  ack?:       { message_id: string; outcome: string };
}

interface BrokerClient {
  Publish(
    req: PublishRequest,
    cb: (err: ServiceError | null, res: PublishResponse) => void,
  ): void;
  Subscribe(): ClientDuplexStream<ConsumerMessage, SubscribeEvent>;
  close(): void;
}

interface ConsumerStats {
  received: number;
  acked:    number;
}

interface ConsumerEntry {
  stream:   ClientDuplexStream<ConsumerMessage, SubscribeEvent>;
  queue:    string;
  prefetch: number;
  stats:    ConsumerStats;
}

interface PublisherStats {
  published: number;
  errors:    number;
}

interface PublishedItem {
  msg_id:   string;
  order_id: string;
  product:  string;
  status:   string;
}

// Request body types
interface PublishBody {
  queue?:   string;
  count?:   number;
  payload?: Partial<OrderEvent>;
}

interface ConsumerStartBody {
  queue?:    string;
  prefetch?: number;
  id?:       string;
}

interface ConsumerStopBody {
  id?: string;
}

// ---------------------------------------------------------------------------
// Structured logger
// ---------------------------------------------------------------------------
const LEVELS = { INFO: 'INFO ', WARN: 'WARN ', ERROR: 'ERROR' } as const;

function log(level: string, component: string, msg: string, fields: LogFields = {}): void {
  const ts = new Date().toISOString();
  const kv = Object.entries(fields)
    .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
    .join(' ');
  process.stdout.write(`${ts} ${level} [${component}] ${msg}${kv ? '  ' + kv : ''}\n`);
}

const logger = {
  info:  (component: string, msg: string, fields?: LogFields) => log(LEVELS.INFO,  component, msg, fields),
  warn:  (component: string, msg: string, fields?: LogFields) => log(LEVELS.WARN,  component, msg, fields),
  error: (component: string, msg: string, fields?: LogFields) => log(LEVELS.ERROR, component, msg, fields),
};

// ---------------------------------------------------------------------------
// gRPC client
// ---------------------------------------------------------------------------
const PROTO_PATH = path.resolve(__dirname, '..', '..', 'proto', 'broker.proto');
const pkgDef     = loader.loadSync(PROTO_PATH, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});

const { broker } = grpc.loadPackageDefinition(pkgDef) as unknown as {
  broker: { Broker: new (addr: string, creds: grpc.ChannelCredentials) => BrokerClient };
};

const grpcClient: BrokerClient = new broker.Broker(
  BROKER_ADDR,
  grpc.credentials.createInsecure(),
);
logger.info('grpc', 'client created', { broker: BROKER_ADDR });

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const publisherStats: PublisherStats = { published: 0, errors: 0 };
const consumers = new Map<string, ConsumerEntry>();
let consumerSeq = 0;

// ---------------------------------------------------------------------------
// Order event factory
// ---------------------------------------------------------------------------
const PRODUCTS = ['widget-A', 'gadget-B', 'doohickey-C', 'thingamajig-D'] as const;
const STATUSES = ['pending', 'confirmed', 'shipped'] as const;

function makeOrder(seq: number, overrides: Partial<OrderEvent> = {}): OrderEvent {
  return {
    order_id:   `ORD-${String(seq).padStart(5, '0')}`,
    product:    PRODUCTS[seq % PRODUCTS.length],
    quantity:   (seq % 5) + 1,
    status:     STATUSES[seq % STATUSES.length],
    created_at: new Date().toISOString(),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Publisher
// ---------------------------------------------------------------------------
function publishOne(
  queue:   string,
  payload: OrderEvent,
  headers: GrpcHeader[] = [],
): Promise<string> {
  return new Promise((resolve, reject) => {
    grpcClient.Publish(
      { queue, payload: Buffer.from(JSON.stringify(payload)), headers },
      (err, res) => {
        if (err) return reject(err);
        resolve(res.message_id);
      },
    );
  });
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------
function startConsumer(queue: string, prefetch = 5, id: string | null = null): string {
  const consumerId = id ?? `consumer-${++consumerSeq}`;
  const stats: ConsumerStats = { received: 0, acked: 0 };
  const stream = grpcClient.Subscribe();

  stream.write({ payload: 'subscribe', subscribe: { queue, prefetch_limit: prefetch } });
  logger.info('consumer', 'subscribed', { id: consumerId, queue, prefetch });

  stream.on('data', (event: SubscribeEvent) => {
    const msg = event.message;
    if (!msg) return;

    let order: Partial<OrderEvent> & { raw?: string };
    try   { order = JSON.parse(msg.payload.toString()) as Partial<OrderEvent>; }
    catch { order = { raw: msg.payload.toString() }; }

    stats.received++;
    const shortId = msg.id.slice(0, 8) + '…';
    logger.info('consumer', 'message received', {
      consumer_id: consumerId,
      id:          shortId,
      queue:       msg.queue,
      retry:       msg.retry_count,
      order_id:    String(order.order_id  ?? '?'),
      product:     String(order.product   ?? '?'),
      status:      String(order.status    ?? '?'),
    });

    setTimeout(() => {
      stream.write({ payload: 'ack', ack: { message_id: msg.id, outcome: 'ACK' } });
      stats.acked++;
      logger.info('consumer', 'ACK sent', { consumer_id: consumerId, id: shortId });
    }, 50);
  });

  stream.on('error', (err: ServiceError) => {
    if (err.code !== grpc.status.CANCELLED) {
      logger.error('consumer', 'stream error', { consumer_id: consumerId, err: err.message });
    }
  });

  stream.on('end', () => {
    logger.info('consumer', 'stream ended', { consumer_id: consumerId });
    consumers.delete(consumerId);
  });

  consumers.set(consumerId, { stream, queue, prefetch, stats });
  return consumerId;
}

function stopConsumer(id: string): boolean {
  const entry = consumers.get(id);
  if (!entry) return false;
  entry.stream.cancel();
  consumers.delete(id);
  logger.info('consumer', 'stopped', { consumer_id: id });
  return true;
}

function stopAllConsumers(): void {
  for (const id of consumers.keys()) stopConsumer(id);
}

// ---------------------------------------------------------------------------
// Express HTTP API
// ---------------------------------------------------------------------------
const app = express();
app.use(express.json());

app.use((req: Request, _res: Response, next: NextFunction) => {
  logger.info('http', `${req.method} ${req.path}`, { ip: req.ip });
  next();
});

// GET /health
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok', broker: BROKER_ADDR });
});

// GET /status
app.get('/status', (_req: Request, res: Response) => {
  const consumerList = Array.from(consumers.entries()).map(([id, c]) => ({
    id,
    queue:    c.queue,
    prefetch: c.prefetch,
    ...c.stats,
  }));
  res.json({
    broker:    BROKER_ADDR,
    publisher: { ...publisherStats },
    consumers: consumerList,
  });
});

// POST /publish
app.post('/publish', async (req: Request<{}, {}, PublishBody>, res: Response) => {
  const queue  = req.body.queue   ?? DEFAULT_Q;
  const count  = Number(req.body.count ?? 1);
  const custom = req.body.payload ?? {};

  if (!Number.isInteger(count) || count < 1 || count > 1000) {
    res.status(400).json({ error: 'count must be an integer between 1 and 1000' });
    return;
  }

  logger.info('publisher', 'publish request', { queue, count });
  const published: PublishedItem[] = [];

  for (let i = 1; i <= count; i++) {
    const order = makeOrder(publisherStats.published + i, custom);
    try {
      const msgId = await publishOne(queue, order, [{ key: 'source', value: 'node-client' }]);
      publisherStats.published++;
      published.push({ msg_id: msgId, order_id: order.order_id, product: order.product, status: order.status });
      logger.info('publisher', 'published', {
        msg_id: msgId, queue, order_id: order.order_id, product: order.product, status: order.status,
      });
    } catch (err) {
      publisherStats.errors++;
      logger.error('publisher', 'publish failed', { err: String(err), queue, seq: i });
      res.status(502).json({ error: String(err), published });
      return;
    }
  }

  res.status(201).json({ published });
});

// POST /consumer/start
app.post('/consumer/start', (req: Request<{}, {}, ConsumerStartBody>, res: Response) => {
  const queue    = req.body.queue    ?? DEFAULT_Q;
  const prefetch = Number(req.body.prefetch ?? 5);
  const id       = req.body.id       ?? null;

  if (id && consumers.has(id)) {
    res.status(409).json({ error: `consumer "${id}" already running` });
    return;
  }

  const consumerId = startConsumer(queue, prefetch, id);
  res.status(200).json({ message: 'consumer started', id: consumerId, queue, prefetch });
});

// POST /consumer/stop
app.post('/consumer/stop', (req: Request<{}, {}, ConsumerStopBody>, res: Response) => {
  const id = req.body.id ?? null;

  if (id) {
    if (!consumers.has(id)) {
      res.status(404).json({ error: `consumer "${id}" not found` });
      return;
    }
    const stats = consumers.get(id)!.stats;
    stopConsumer(id);
    res.json({ message: 'consumer stopped', id, stats });
    return;
  }

  if (consumers.size === 0) {
    res.status(409).json({ error: 'no consumers are running' });
    return;
  }

  const snapshot = Array.from(consumers.entries()).map(([cid, c]) => ({ id: cid, stats: c.stats }));
  stopAllConsumers();
  res.json({ message: 'all consumers stopped', stopped: snapshot });
});

// ---------------------------------------------------------------------------
// Start server
// ---------------------------------------------------------------------------
const server = app.listen(HTTP_PORT, () => {
  logger.info('http', 'server listening', { port: HTTP_PORT });
  logger.info('http', 'endpoints', {
    health:        `GET  http://localhost:${HTTP_PORT}/health`,
    status:        `GET  http://localhost:${HTTP_PORT}/status`,
    publish:       `POST http://localhost:${HTTP_PORT}/publish`,
    consumerStart: `POST http://localhost:${HTTP_PORT}/consumer/start`,
    consumerStop:  `POST http://localhost:${HTTP_PORT}/consumer/stop`,
  });
});

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------
function shutdown(sig: string): void {
  logger.info('app', `${sig} received, shutting down`);
  stopAllConsumers();
  grpcClient.close();
  server.close(() => {
    logger.info('app', 'HTTP server closed');
    process.exit(0);
  });
}

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
