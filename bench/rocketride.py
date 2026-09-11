"""Inspect and validate the RocketRide Cloud runtime without printing credentials."""
from __future__ import annotations
import argparse
import asyncio
import json
import os
from pathlib import Path
import config  # noqa: F401
from rocketride import RocketRideClient


def redact(value):
    text = json.dumps(value, default=str)
    for key in ('ROCKETRIDE_API_KEY', 'HOTDATA_API_KEY', 'OPENAI_API_KEY'):
        secret = os.getenv(key)
        if secret:
            text = text.replace(secret, '[REDACTED]')
    return json.loads(text)


async def inspect_runtime(uri, smoke=False):
    async with RocketRideClient(uri=uri, auth=os.environ['ROCKETRIDE_API_KEY'],
                                env={}, request_timeout=60000, max_retry_time=15000) as client:
        print('RocketRide connected:', uri, flush=True)
        if smoke:
            from agents.rocketride import build_pipeline, bind_secrets
            from rocketride.schema import Question
            pipeline = bind_secrets(build_pipeline(smoke=True))
            validation = await client.validate(pipeline)
            print('Validation:', redact(validation), flush=True)
            started = await client.use(pipeline=pipeline, ttl=900, pipelineTraceLevel='full', name='IncidentSwarm integration smoke')
            token = started['token']
            try:
                schema = await client.tool(token=token, node_id='db_probe', tool='get_schema')
                print('Database schema:', redact(schema), flush=True)
                loaded = await client.tool(token=token, node_id='db_probe', tool='load_data', input={'table':'probe','rows':[{'n':1},{'n':2}]})
                print('Loaded:', redact(loaded), flush=True)
                question = Question()
                question.addQuestion('Count the actual rows in probe using your database tool. Return JSON.')
                result = await client.chat(token=token, question=question)
                Path('data/rocketride_smoke.json').write_text(json.dumps(redact(result),indent=2))
                print('Smoke response saved; fields:', list(result), flush=True)
            finally:
                await client.terminate(token)
                print('Smoke pipeline terminated', flush=True)
            return
        services = await client.get_services()
        out = Path('data/rocketride_services.json')
        out.write_text(json.dumps(redact(services), indent=2))
        print('Service catalog saved:', out)
        print('Top-level catalog fields:', list(services))
        for name in ('agent_rocketride', 'memory_internal', 'llm_openai', 'db_hotdata', 'chat', 'response_answers'):
            spec = await client.get_service(name)
            Path(f'data/rocketride_{name}.json').write_text(json.dumps(redact(spec), indent=2))
            print(name, 'schema saved')
        templates = await client.get_all_templates()
        Path('data/rocketride_templates.json').write_text(json.dumps(redact(templates), indent=2))
        print('Templates saved')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--uri', default=os.getenv('ROCKETRIDE_URI', 'https://staging.rocketride.ai'))
    ap.add_argument('--smoke', action='store_true')
    args = ap.parse_args()
    try:
        asyncio.run(inspect_runtime(args.uri, args.smoke))
    except Exception as exc:
        print('RocketRide check failed:', redact(str(exc)))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
