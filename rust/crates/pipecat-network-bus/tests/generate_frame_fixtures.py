"""Generate frame fixtures from source definitions and the actual Python bus codec.

Optional provider imports are excluded with AST selection; pydantic is required
for the original VAD/turn parameter classes. No Python runs in the Rust codec.
"""
import ast
import base64
import dataclasses
import enum
import itertools
import json
import re
import sys
import types
import typing
from pathlib import Path
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[4]
OUT = Path(__file__).parent / 'fixtures' / 'python_frames.json'
ids=itertools.count(5000)
counts={}
def count(value):
    name=type(value).__name__
    counts[name]=counts.get(name,-1)+1
    return counts[name]
class Log:
    @staticmethod
    def warning(*args, **kwargs): pass
logger=Log()
NOT_GIVEN=object()
def load(relative,module_name,selected=None,**extra):
    module=types.ModuleType(module_name);sys.modules[module_name]=module
    module.__dict__.update(dataclass=dataclasses.dataclass,field=dataclasses.field,
        Enum=enum.Enum,StrEnum=enum.StrEnum,BaseModel=BaseModel,Any=typing.Any,ClassVar=typing.ClassVar,
        obj_id=lambda:next(ids),obj_count=count,Generic=typing.Generic,TSettings=typing.TypeVar('TSettings'),
        deprecated=lambda *a,**k: lambda cls: cls,warn_deprecated=lambda *a,**k:None,
        time=types.SimpleNamespace(time=lambda:1720000000.25),NOT_GIVEN=NOT_GIVEN,**extra)
    tree=ast.parse((ROOT/relative).read_text())
    # Pull literal module constants from the source too, so default changes
    # cannot be hidden by a copied numeric/string value in this oracle.
    for node in tree.body:
        if isinstance(node, ast.Assign):
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    module.__dict__[target.id] = value
    nodes=[n for n in tree.body if isinstance(n,ast.ClassDef) and (selected is None or n.name in selected)]
    body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),*nodes]
    exec(compile(ast.fix_missing_locations(ast.Module(body=body,type_ignores=[])),str(ROOT/relative),'exec'),module.__dict__)
    return module
language=load('src/pipecat/transcriptions/language.py','pipecat.transcriptions.language',{'Language'}).Language
keypad=load('src/pipecat/audio/dtmf/types.py','pipecat.audio.dtmf.types').KeypadEntry
error_category=load('src/pipecat/utils/errors.py','pipecat.utils.errors',{'ErrorCategory'}).ErrorCategory
aggregation=load('src/pipecat/utils/text/base_text_aggregator.py','pipecat.utils.text.base_text_aggregator',{'AggregationType'}).AggregationType
settings=load('src/pipecat/services/settings.py','pipecat.services.settings',{'ServiceSettings','LLMSettings','TTSSettings','STTSettings'})
completion=load('src/pipecat/turns/user_turn_completion_mixin.py','pipecat.turns.user_turn_completion_mixin',{'UserTurnCompletionConfig'}).UserTurnCompletionConfig
summary=load('src/pipecat/utils/context/llm_context_summarization.py','pipecat.utils.context.llm_context_summarization',{'LLMContextSummaryConfig'}).LLMContextSummaryConfig
vad=load('src/pipecat/audio/vad/vad_analyzer.py','pipecat.audio.vad.vad_analyzer',{'VADParams'}).VADParams
turn=load('src/pipecat/audio/turn/base_turn_analyzer.py','pipecat.audio.turn.base_turn_analyzer',{'BaseTurnParams'}).BaseTurnParams
frames=load('src/pipecat/frames/frames.py','pipecat.frames.frames',KeypadEntry=keypad,ServiceSettings=settings.ServiceSettings,LLMSettings=settings.LLMSettings,TTSSettings=settings.TTSSettings,STTSettings=settings.STTSettings)
bus=load('src/pipecat/bus/messages.py','pipecat.bus.messages')
direction=load('src/pipecat/processors/frame_processor.py','pipecat.processors.frame_processor',{'FrameDirection'}).FrameDirection
function=load('src/pipecat/adapters/schemas/function_schema.py','pipecat.adapters.schemas.function_schema',{'FunctionSchema'}).FunctionSchema
tools=load('src/pipecat/adapters/schemas/tools_schema.py','pipecat.adapters.schemas.tools_schema',{'AdapterType','ToolsSchema'},FunctionSchema=function)
tools_adapter=load('src/pipecat/bus/adapters/tools_schema_adapter.py','pipecat.bus.adapters.tools_schema_adapter',{'ToolsSchemaAdapter'},TypeAdapter=object,FunctionSchema=function,ToolsSchema=tools.ToolsSchema,AdapterType=tools.AdapterType).ToolsSchemaAdapter
class Context:
    def __init__(self,messages=None,tools=NOT_GIVEN,tool_choice=NOT_GIVEN):self.messages,self.tools,self.tool_choice=messages or [],tools,tool_choice
Context.__name__='LLMContext';Context.__module__='pipecat.processors.aggregators.llm_context'
context_module=types.ModuleType(Context.__module__);context_module.LLMContext=Context;sys.modules[Context.__module__]=context_module
context_adapter=load('src/pipecat/bus/adapters/llm_context_adapter.py','pipecat.bus.adapters.llm_context_adapter',{'LLMContextAdapter'},TypeAdapter=object,ToolsSchemaAdapter=tools_adapter,is_given=lambda x:x is not NOT_GIVEN,LLMSpecificMessage=type('Specific',(),{}),LLMContext=Context).LLMContextAdapter
# Resolve only the actual source classes selected above; never import providers.
def resolve(name):
    module,_,name=name.rpartition('.')
    return getattr(sys.modules.get(module),name,None)
serializer_class=load('src/pipecat/bus/serializers/json.py','pipecat.bus.serializers.json',{'JSONMessageSerializer'},MessageSerializer=object,dataclasses=dataclasses,base64=base64,json=json,logger=logger,_JSON_NATIVE=(str,int,float,bool,type(None)),_resolve_type=resolve).JSONMessageSerializer
serializer=serializer_class.__new__(serializer_class)
serializer._adapters={Context:context_adapter(),tools.ToolsSchema:tools_adapter()}
standard=function('lookup','Look up a record',{'key':{'type':'string'}},['key'])
tool_schema=tools.ToolsSchema([standard],{tools.AdapterType.GEMINI:[{'google_search':{}}]})
context=Context([{'role':'user','content':[{'type':'text','text':'hello 世界'},{'type':'image_url','image_url':{'url':'https://example.invalid/image.png','detail':'high'}}], 'provider_flag':{'retained':True}}],tool_schema,{'type':'function','function':{'name':'lookup'}})
args={
    'text':'hello 世界','audio':b'\x01\x00\xff\x7f','sample_rate':16000,'num_channels':1,
    'image':b'\x00\x01\xfe\xff'*4,'size':[2,2],'format':'RGBA',
    'file':'data:application/pdf;base64,AAE=','type':'bytes','filename':'example.pdf',
    'aggregated_by':aggregation.SENTENCE,'context_id':'ctx-1','raw_text':'<s>hello 世界</s>',
    'user_id':'user-9','timestamp':'2026-10-09T17:00:00Z','language':language.EN_US,'result':{'provider':{'words':['hello']}},'finalized':True,
    'marker':'●','raw':'● hello','kind':'complete','markers':['●','◐','○'],'segment_id':42,
    'accumulated_text':'hello','remaining_text':' 世界','context':context,'speculation':True,
    'messages':context.messages,'run_llm':False,'tools':tool_schema,'tool_choice':{'type':'function','function':{'name':'lookup'}},
    'enable':True,'skip_tts':True,'function_name':'lookup','tool_call_id':'call-1','arguments':{'key':'abc'},
    'properties':frames.FunctionCallResultProperties(run_llm=False,is_final=False),'error':'example failure','is_final':False,
    'append_to_context':False,'message':{'payload':b'\x00\xff'},'button':keypad.ONE,'buttons':[keypad.ONE,keypad.STAR,keypad.POUND],
    'audio_in_sample_rate':8000,'audio_out_sample_rate':48000,'enable_metrics':True,'enable_tracing':True,'enable_usage_metrics':True,'report_only_initial_ttfb':True,
    'llm':'provider-key','signature':{'signed':'signature'},'reason':{'code':'done','count':3},'fatal':False,'category':error_category.RATE_LIMIT,
    'start_secs':0.3,'stop_secs':0.8,'mute':True,'video_source':'camera','original_data':b'\x89PNG','original_mime_type':'image/png',
    'custom_options':{'pdf':{'pages':[1,2]}},'vad_params':vad(confidence=0.9,start_secs=0.4,stop_secs=0.7,min_volume=0.5),
    'turn_params':turn(),'service_name':'test-service','ttfs_p99_latency':0.15,'is_realtime_service':True,
    'returning':True,'origin':'bridge','config':summary(target_context_tokens=1000,min_messages_after_summary=2,summarization_prompt='summarize',summarization_timeout=9.0),
    'request_id':'summary-1','min_messages_to_keep':2,'target_context_tokens':1000,'summarization_prompt':'summarize','summarization_timeout':9.0,
    'summary':'summary text','last_summarized_index':8,'cancel_on_interruption':True,'group_id':'group-1',
    'settings':{'nested':{'field':3}},'reach_inactive_services':True,'timeout':30.5,'params':vad(confidence=0.8),'volume':0.5,'gain':1.5,
}
# Enumerate only concrete frame classes implemented by the Rust allowlist.
extended=(Path(__file__).parent.parent / 'src' / 'frame_extended.rs').read_text().split('pub(super) const TYPES:')[1].split('];')[0]
original=(ROOT/'rust/crates/pipecat-network-bus/src/frames.rs').read_text().split('pub(crate) fn supported_frame')[1].split('pub(crate) fn decode_bytes')[0]
allowed=set(re.findall(r'"([A-Za-z]+Frame)"',extended+original))|{'Frame'}
fixtures=[]
for name in sorted(allowed):
    cls=getattr(frames,name)
    values={f.name:args[f.name] for f in dataclasses.fields(cls) if f.init and f.name in args}
    if name=='HeartbeatFrame':values['timestamp']=1234567890123
    if name.startswith('VADUser'):values['timestamp']=1720000000.25
    if name=='FunctionCallsStartedFrame':values['function_calls']=[frames.FunctionCallFromLLM('lookup','call-1',{'key':'abc'},context)]
    if name=='ExternalFunctionCallsStartedFrame':values['function_calls']=[frames.ExternalFunctionCall('lookup','call-1')]
    if name.endswith('UpdateSettingsFrame') and name not in {'FilterUpdateSettingsFrame','MixerUpdateSettingsFrame'}:
        typename={'ServiceUpdateSettingsFrame':'ServiceSettings','LLMUpdateSettingsFrame':'LLMSettings','TTSUpdateSettingsFrame':'TTSSettings','STTUpdateSettingsFrame':'STTSettings'}[name]
        delta_values={'model':'model-v2','extra':{'vendor':3}}
        if typename=='LLMSettings':delta_values.update(system_instruction='system',temperature=0.5,max_tokens=42,top_p=0.9,top_k=7,frequency_penalty=0.25,presence_penalty=0.5,seed=-3,filter_incomplete_user_turns=True,user_turn_completion_config=completion(instructions='complete?',incomplete_short_timeout=3.0))
        if typename=='TTSSettings':delta_values.update(voice='voice-v2',language=language.EN_US)
        if typename=='STTSettings':delta_values.update(language=language.EN_US)
        values['delta']=getattr(settings,typename)(**delta_values)
    obj=cls(**values)
    for field,value in [('skip_tts',True),('includes_inter_frame_spaces',True),('append_to_context',False),('will_be_spoken',True),('sync_with_audio',True)]:
        if hasattr(obj,field):setattr(obj,field,value)
    obj.metadata={'fixture':name,'bytes':b'\x00\xff'};obj.pts=9001;obj.broadcast_sibling_id=9002;obj.transport_source='microphone';obj.transport_destination='speaker'
    message=bus.BusFrameMessage(source='python-source',target='rust-target',frame=obj,direction=direction.UPSTREAM,bridge='media')
    wire=json.loads(serializer.serialize(message))
    restored=serializer.deserialize(json.dumps(wire).encode())
    # The actual Python decoder accepts list-valued dimensions. Image consumers
    # index/unpack them; neither construction nor reserialization needs tuples.
    if hasattr(obj,'size'):assert restored.frame.size==[2,2]
    assert json.loads(serializer.serialize(restored))==wire,name
    fixtures.append(wire)
# Source defaults and populated unsafe objects expose omissions that Rust must
# not pretend to reconstruct. Keep them separate from valid wire fixtures.
image=frames.InputImageRawFrame(b'\x00', (1,1), 'RGB')
assert 'size' not in json.loads(serializer.serialize(bus.BusFrameMessage(source='source',frame=image,direction=direction.DOWNSTREAM)))['__data__']['frame']['__data__']
# Native emitters can repair Python's lossy omission of None settings with an
# explicit JSON null; the source decoder reconstructs the intended clear.
clear=serializer._deserialize_value({'__type__':'pipecat.services.settings.LLMSettings','__data__':{'model':None,'extra':{}}})
assert clear.model is None
assert clear.temperature is NOT_GIVEN
tracing=load('src/pipecat/utils/tracing/tracing_context.py','pipecat.utils.tracing.tracing_context',{'TracingContext'},is_tracing_available=lambda:False).TracingContext()
start=frames.StartFrame(enable_tracing=True,tracing_context=tracing)
start_wire=serializer._serialize_value(start)
assert start_wire['__data__']['enable_tracing'] is True
assert 'tracing_context' not in start_wire['__data__']
callback_frame=frames.FunctionCallResultFrame('lookup','call',{}, {}, properties=frames.FunctionCallResultProperties(on_context_updated=lambda:None))
assert 'on_context_updated' not in serializer._serialize_value(callback_frame)['__data__']['properties']['__data__']
OUT.write_text(json.dumps(fixtures,ensure_ascii=False,indent=2)+'\n')
print(f'Wrote {len(fixtures)} source frame fixtures to {OUT}')
