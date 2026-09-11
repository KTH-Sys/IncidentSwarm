"""Portable RocketRide graphs; secrets are bound only in memory at launch."""
from __future__ import annotations
import copy
import os
import uuid
from pathlib import Path
import json

from agents.agent import SCHEMA_HINT
from agents.hotdata_scope import SLICES
from agents.llm import load_prompt

NAMESPACE = uuid.UUID('8f12d536-8910-4f18-a03e-36b847e4a5f0')


def node(node_id, provider, config, *, inputs=None, control=None, x=0, y=0):
    out = {'id': node_id, 'provider': provider, 'config': config,
           'position': {'x': x, 'y': y}}
    if inputs:
        out['input'] = inputs
    if control:
        out['control'] = control
    return out


def control(kind, parent):
    return {'classType': kind, 'from': parent}


def model_node(name, model, consumers, x, y):
    profile = 'openai-5-mini' if model == 'fast' else 'openai-5'
    return node(name, 'llm_openai', {'profile': profile, profile: {'apikey': '${OPENAI_API_KEY}'}},
                control=[control('llm', parent) for parent in consumers], x=x, y=y)


def database_node(agent, x, y):
    return node(f'db_{agent}', 'db_hotdata', {
        'type': 'db_hotdata', 'apikey': '${HOTDATA_API_KEY}',
        'workspace_id': '${HOTDATA_WORKSPACE}', 'ttl': '1h',
        'table': SLICES.get(agent, {}).get('table', 'probe'),
        'allow_execute': True, 'allow_destructive_load': False,
        'max_attempts': 2, 'max_execute_rows': 40,
        'job_timeout_secs': 120, 'async_after_ms': 5000,
        'db_description': 'Read-only incident evidence. Never modify or load data during investigation.'
    }, control=[control('tool', f'agent_{agent}')], x=x, y=y)


def build_pipeline(mode='parallel', *, smoke=False):
    agents = ['probe'] if smoke else (['logs', 'metrics', 'changes', 'infra'] if mode == 'parallel' else ['single'])
    components = [node('chat', 'chat', {'hideForm': True, 'type': 'chat', 'mode': 'Source', 'parameters': {}}, x=0, y=0)]
    parent = 'coordinator' if mode == 'parallel' and not smoke else f'agent_{agents[0]}'
    if parent == 'coordinator':
        instructions = [
            'Coordinate incident investigation. Your FIRST execution wave MUST invoke all four specialist run_agent tools together in ONE tool_calls array. Do not call specialists sequentially.',
            'Pass the same symptom summary to each specialist. Specialists have isolated databases. Collect all four results before answering. Never invent a missing tool result.',
            'Return bare JSON with two keys: findings (array of the four HypothesisSet results) and report (RCAReport). Use memory references to include complete specialist results.',
            load_prompt('correlator'),
        ]
        components += [node(parent, 'agent_rocketride', {'agent_description': 'Parallel incident commander',
            'instructions': instructions, 'max_waves': 5, 'require_tool_call': True},
            inputs=[{'lane':'questions','from':'chat'}], x=300,y=0),
            node('memory_coordinator','memory_internal',{'type':'memory_internal'},control=[control('memory',parent)],x=300,y=250),
            model_node('llm_strong','strong',[parent],300,-250)]
    consumers=[]
    for i, agent in enumerate(agents):
        aid=f'agent_{agent}'
        if smoke:
            instructions=['Use db_probe.execute to run SELECT COUNT(*) AS n FROM probe. Return the actual count as a JSON object. Use no other tools.']
        else:
            tables = [v['table'] for v in SLICES.values()] if agent == 'single' else [SLICES[agent]['table']]
            instructions=[load_prompt(agent), '\n'.join(SCHEMA_HINT[t] for t in tables),
                f'Your database tool is db_{agent}. Use execute for SQL, NOT run_sql. Data is already loaded. Never call load_data or build_index. Use at most 3 investigation waves, group independent queries in the same wave, and then return your complete JSON findings.',
                'Search SQL uses SELECT * FROM bm25_search(\'logs\', \'msg\', \'query\') LIMIT 20. Prefer SQL aggregation, and use LIKE if search is unavailable.']
        components += [node(aid,'agent_rocketride', {'agent_description': f'{agent} incident specialist',
            'instructions':instructions,'max_waves':4,'require_tool_call':True},
            inputs=[{'lane':'questions','from':'chat'}] if aid == parent else None,
            control=[control('tool',parent)] if aid != parent else None,x=650,y=i*300),
            node(f'memory_{agent}','memory_internal',{'type':'memory_internal'},control=[control('memory',aid)],x=950,y=i*300),
            database_node(agent,950,i*300+100)]
        consumers += [aid,f'db_{agent}']
    components += [model_node('llm_fast','fast',consumers,1250,0),
                   node('response','response_answers',{'laneName':'answers'},inputs=[{'lane':'answers','from':parent}],x=1550,y=0)]
    return {'project_id':str(uuid.uuid5(NAMESPACE,f'{mode}-{smoke}')), 'version':1,
            'source':'chat','components':components,'viewport':{'x':0,'y':0,'zoom':0.6}}


def bind_secrets(pipeline):
    result = copy.deepcopy(pipeline)
    def bind(value):
        if isinstance(value,dict): return {k:bind(v) for k,v in value.items()}
        if isinstance(value,list): return [bind(v) for v in value]
        if isinstance(value,str) and value.startswith('${') and value.endswith('}'):
            key=value[2:-1]
            if not os.getenv(key): raise ValueError(f'{key} is required')
            return os.environ[key]
        return value
    return bind(result)


def export_pipelines():
    for mode in ('parallel','single'):
        path=Path(f'pipes/incident_{mode}.pipe')
        path.write_text(json.dumps(build_pipeline(mode),indent=2)+'\n')
        print('Exported',path)
