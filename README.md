# Platform Service

A Node.js service built with TypeScript and Fastify.

## Features

- 🚀 **Fastify** - Fast and low overhead web framework
- 📘 **TypeScript** - Type-safe development
- 🔄 **Hot Reload** - Auto-restart on file changes with ts-node-dev
- 🌐 **CORS** - Configured with @fastify/cors
- 📝 **Logging** - Built-in request logging with Pino
- ⚙️ **Environment Variables** - Configuration via .env files

## Prerequisites

- Node.js (v16 or higher)
- npm or yarn

## Getting Started

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure Environment

Create a `.env` file in the root directory:

```env
PORT=3000
HOST=0.0.0.0
LOG_LEVEL=info
```

### 3. Run Development Server

```bash
npm run dev
```

The server will start on `http://localhost:3000`

### 4. Build for Production

```bash
npm run build
```

### 5. Start Production Server

```bash
npm start
```

## Available Scripts

- `npm run dev` - Start development server with hot reload
- `npm run build` - Build TypeScript to JavaScript
- `npm start` - Start production server
- `npm run lint` - Lint code with ESLint
- `npm run format` - Format code with Prettier

## API Endpoints

### Health Check
```
GET /health
```
Returns the health status of the service.

### Hello World
```
GET /api/hello
```
Returns a welcome message.

### Create Item
```
POST /api/items
Content-Type: application/json

{
  "name": "Item Name",
  "description": "Optional description"
}
```
Creates a new item and returns it with an ID.

## Project Structure

```
platform/
├── src/
│   └── index.ts          # Main application entry point
├── dist/                 # Compiled JavaScript (generated)
├── node_modules/         # Dependencies (generated)
├── .gitignore           # Git ignore rules
├── package.json         # Project dependencies and scripts
├── tsconfig.json        # TypeScript configuration
└── README.md            # This file
```

## Development

### Adding New Routes

You can add new routes in `src/index.ts` or create separate route files:

```typescript
fastify.get('/api/example', async (request, reply) => {
  return { message: 'Example endpoint' };
});
```

### Type Safety

Use TypeScript interfaces for request/response types:

```typescript
interface MyRequestBody {
  field: string;
}

fastify.post<{ Body: MyRequestBody }>('/api/endpoint', async (request, reply) => {
  const { field } = request.body;
  // TypeScript knows the shape of request.body
});
```

## License

MIT
