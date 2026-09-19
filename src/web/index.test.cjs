const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

// Run the actual embedded panel script against a minimal DOM. No browser
// storage, management credentials or upstream network calls are involved.
class Element {
  constructor(tag='div') {
    this.tagName=tag; this.children=[]; this.style={setProperty(name,value){this[name]=value;},removeProperty(name){delete this[name];}}; this.attributes={}; this.className=''; this.events={}; this.text='';
    this.classList={
      contains:name=>this.className.split(' ').includes(name),
      add:name=>{if(!this.classList.contains(name))this.className+=' '+name;},
      remove:name=>{this.className=this.className.split(' ').filter(item=>item!==name).join(' ');},
      toggle:(name,force)=>{const on=force??!this.classList.contains(name);this.classList[on?'add':'remove'](name);return on;}
    };
  }
  set textContent(value){this.text=String(value??'');this.children=[];}
  get textContent(){return this.text+this.children.map(child=>child.textContent).join('');}
  append(...items){for(const item of items){if(item.tagName==='fragment')this.children.push(...item.children);else this.children.push(item);}}
  replaceChildren(...items){this.text='';this.children=[];this.append(...items);}
  addEventListener(name,fn){this.events[name]=fn;}
  setAttribute(name,value){this.attributes[name]=value;}
  getAttribute(name){return this.attributes[name]??null;}
  scrollIntoView(){}
  focus(){this.focused=true;}
}

function panel(options={}) {
  const nodes=new Map();
  const get=id=>{if(!nodes.has(id))nodes.set(id,new Element());return nodes.get(id);};
  const root=new Element('html'),host=new Element('html'),windowEvents={},mediaEvents={};
  if(options.hostTheme)host.setAttribute('data-theme',options.hostTheme);
  let storedTheme=options.storedTheme;
  const media={matches:!!options.darkSystem,addEventListener:(name,fn)=>{mediaEvents[name]=fn;}};
  let onMutation;
  const win={addEventListener:(name,fn)=>{(windowEvents[name]??=[]).push(fn);},matchMedia:()=>media};
  win.parent=options.embedded?{document:{documentElement:host},getComputedStyle:()=>({getPropertyValue:name=>options.hostTokens?.[name]||''})}:win;
  if(options.crossOrigin)Object.defineProperty(win,'parent',{get(){throw new Error('cross origin');}});
  const context=vm.createContext({
    document:{documentElement:root,getElementById:get,createElement:tag=>new Element(tag),createDocumentFragment:()=>new Element('fragment')},
    location:{pathname:'/management.html',host:'localhost',origin:'http://localhost'},
    navigator:{userAgent:'panel-test'},localStorage:{getItem:name=>name==='cli-proxy-theme'?JSON.stringify({state:{theme:storedTheme}}):null},
    window:win,MutationObserver:class{constructor(fn){onMutation=fn;}observe(){}disconnect(){}},
    setInterval(){},setTimeout(){},URL,TextEncoder,TextDecoder,
  });
  const html=fs.readFileSync(__dirname+'/index.html','utf8');
  const script=html.match(/<script>([\s\S]*?)<\/script>/)[1];
  vm.runInContext(script,context);
  vm.runInContext('globalThis.renderTest=renderProbe;globalThis.renderPoolTest=renderPool;globalThis.openExitTest=openExitEditor;globalThis.renderDataTest=render;globalThis.actions=[];globalThis.controlResult=false;probeControl=async payload=>{actions.push(payload);return controlResult;}',context);
  return {get,root,host,changeHost:theme=>{host.setAttribute('data-theme',theme);onMutation();},changeStored:theme=>{storedTheme=theme;for(const fn of windowEvents.storage||[])fn({key:'cli-proxy-theme'});},changeSystem:dark=>{media.matches=dark;mediaEvents.change();},render:context.renderTest,renderPool:context.renderPoolTest,openExit:context.openExitTest,renderData:context.renderDataTest,actions:context.actions,setResult:value=>{context.controlResult=value;}};
}

function fixture(overrides={}) {
  return {enabled:true,running:false,halted:false,prefetch_minutes:3,paused:[],active_models:[],queue_models:[],
    values:[{model:'gpt-6-astra',valid:true,value_length:292,source:'probe',remaining_seconds:180,
      issued_at:new Date(Date.now()-52*60000).toISOString(),expires_at:new Date(Date.now()+3*60000).toISOString()}],...overrides};
}

test('full stop and zero prefetch window do not advertise automatic takeover',()=>{
  const p=panel();p.render(fixture({halted:true}));
  assert.equal(p.get('probe-status').textContent,'全部停止');
  assert.match(p.get('probe-times').textContent,/自动预备已停止/);
  assert.doesNotMatch(p.get('probe-times').textContent,/到期前/);
  p.render(fixture({prefetch_minutes:0,pool_attempts:0}));
  assert.equal(p.get('probe-status').textContent,'手动待命');
  assert.match(p.get('probe-times').textContent,/窗口为 0/);
  assert.match(p.get('probe-times').textContent,/池预算 0 次/);
});

test('idle participation, pause and one-off probing remain distinct',()=>{
  const p=panel();p.render(fixture());
  let row=p.get('probe-values').children[0];
  assert.equal(row.classList.contains('paused-row'),false);
  assert.match(row.children[0].textContent,/自动预备待命/);
  assert.equal(row.children[6].children[0].textContent,'暂停');
  p.render(fixture({paused:['gpt-6-astra']}));
  row=p.get('probe-values').children[0];
  assert.equal(row.classList.contains('paused-row'),true);
  assert.equal(row.children[6].children[0].textContent,'恢复');
  assert.equal(row.children[6].children[1].disabled,true);
  p.render(fixture({halted:true,running:true,active_models:['gpt-6-astra']}));
  assert.equal(p.get('probe-status').textContent,'运行中');
  assert.match(p.get('probe-times').textContent,/自动预备已停止/);
  assert.equal(p.get('round-start').disabled,false);
});

test('stopping remains visible until drain and keeps the prefetch mode clear',()=>{
  const p=panel();p.render(fixture({running:true,stopping:true,active_models:['gpt-6-astra']}));
  assert.equal(p.get('probe-status').textContent,'正在停止');
  assert.match(p.get('probe-times').textContent,/本轮结束后继续自动预备/);
  assert.equal(p.get('round-start').disabled,true);
  assert.match(p.get('probe-values').textContent,/正在停止/);
});

test('business evidence takes precedence and rejection control is restored after refresh',()=>{
  const p=panel();p.get('reject-toggle').disabled=true;
  p.render(fixture({failures:[{model:'gpt-6-astra',attempts:3}],business:[{model:'gpt-6-astra',reason:'业务模型不一致'}]}));
  assert.equal(p.get('reject-toggle').disabled,false);
  assert.match(p.get('probe-values').textContent,/业务确认/);
  assert.doesNotMatch(p.get('probe-values').textContent,/上次探测/);
  assert.match(p.get('probe-values').textContent,/剩余 3m 00s/);
});

test('probe failure preserves healthy TTL and expired rows have no stray class text',()=>{
  const p=panel();p.render(fixture({failures:[{model:'gpt-6-astra',attempts:3}]}));
  let ttl=p.get('probe-values').children[0].children[5];
  assert.equal(ttl.className,'ttl-cell');
  assert.match(ttl.textContent,/现有基线继续使用/);
  assert.match(ttl.textContent,/剩余 3m 00s/);
  p.render(fixture({values:[{model:'gpt-6-astra',expired:true,remaining_seconds:-1,value_length:292}]}));
  ttl=p.get('probe-values').children[0].children[5];
  assert.equal(ttl.textContent,'已到期');
});

test('stop buttons send different actions and pause acts on participation',async()=>{
  const p=panel();p.render(fixture({running:true}));
  await p.get('round-stop').events.click();
  await p.get('all-stop').events.click();
  await p.get('probe-values').children[0].children[6].children[0].events.click();
  assert.deepEqual(Array.from(p.actions,item=>item.action),['stop-current','stop-all','pause']);
});

test('unchecked models show policy without stale failure, expiry or controls',()=>{
  const p=panel();
  const models=['gpt-5.6-luna','gpt-5.6-terra'];
  p.render(fixture({models,detection_models:[],prefetch_enabled:false,
    values:models.map(model=>({model,detection_enabled:false,value_length:312,expired:true,candidate:{remaining_seconds:120}})),
    paused:models,active_models:models,failures:models.map(model=>({model,attempts:99})),
    business:models.map(model=>({model,reason:'旧模型不一致'}))}));
  assert.equal(p.get('probe-status').textContent,'不检测');
  assert.equal(p.get('round-start').disabled,true);
  assert.equal(p.get('probe-priority').textContent,'—');
  assert.match(p.get('probe-times').textContent,/当前模型均不检测/);
  for(const row of p.get('probe-values').children){
    assert.equal(row.children.length,7);
    assert.equal(row.children[5].textContent,'不检测');
    assert.equal(row.children[6].children.length,0);
    assert.doesNotMatch(row.textContent,/失败|已到期|暂停|312|预备就绪/);
  }
});

test('unchecked request records retain raw observations without mismatch badges',()=>{
  const p=panel();
  p.renderData({total:1,replaced:0,inserted:0,records:[{
    time:new Date().toISOString(),model:'gpt-5.6-luna',upstream_model:'gpt-6-astra',
    detection_exempt:true,model_checked:true,model_mismatch:true,turn_state_length:312,action:'unchanged'
  }]});
  const row=p.get('rows').children[0];
  assert.match(row.children[2].textContent,/gpt-6-astra/);
  assert.match(row.children[2].textContent,/不检测/);
  assert.doesNotMatch(row.textContent,/模型不一致|降智已拦截/);
  assert.equal(row.children[6].className,'turnstate');
  assert.match(row.children[6].textContent,/312 字节/);
});

test('prefetch draft survives refresh and requires explicit valid confirmation',async()=>{
  const p=panel();p.render(fixture({ttl_minutes:55}));
  assert.equal(p.get('prefetch-minutes').value,'3');
  p.get('prefetch-minutes').value='10';p.get('prefetch-minutes').events.input();
  p.render(fixture({prefetch_minutes:5}));
  assert.equal(p.get('prefetch-minutes').value,'10');
  assert.equal(p.actions.length,0);
  for(const invalid of ['','-1','1.5','55','1e2']){
    p.get('prefetch-minutes').value=invalid;await p.get('prefetch-save').events.click();
    assert.equal(p.actions.length,0);
  }
  p.get('prefetch-minutes').value='10';await p.get('prefetch-save').events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.actions)),[{action:'prefetch-window',minutes:10}]);
  assert.match(p.get('prefetch-feedback').textContent,/保存失败/);
  assert.equal(p.get('prefetch-minutes').value,'10');
});

test('prefetch confirmation remains stable during polling and permits zero',async()=>{
  const p=panel();p.render(fixture({halted:true}));
  let finish;const pending=new Promise(resolve=>{finish=resolve;});p.setResult(pending);
  p.get('prefetch-minutes').value='0';p.get('prefetch-minutes').events.input();
  const saving=p.get('prefetch-save').events.click();
  p.render(fixture({halted:true,prefetch_minutes:3}));
  assert.equal(p.get('prefetch-minutes').value,'0');
  assert.equal(p.get('prefetch-save').disabled,true);
  finish(true);await saving;
  assert.match(p.get('prefetch-feedback').textContent,/已保存：0/);
  assert.equal(p.get('prefetch-save').disabled,false);
  assert.equal(p.actions[0].action,'prefetch-window');
  assert.equal(p.actions.length,1);
});

test('proxy editing retains authentication by omission and uses stable IDs',async()=>{
  const p=panel();
  const item={id:'exit-test',proxy:'socks5://192.0.2.1:1080',label:'备用出口',has_auth:true,pool:false,attempts:0,multiplier:1,budget:3,disabled:true};
  p.renderPool({proxies_state:[item]});
  const row=p.get('pool-rows').children[0];
  assert.match(row.children[0].children[2].textContent,/3 × 1 = 3/);
  await row.children[0].children[3].children[1].events.click();
  assert.equal(p.actions[0].id,'exit-test');assert.equal(p.actions[0].proxy,undefined);
  row.children[0].children[3].children[0].events.click();
  assert.equal(p.get('exit-password').value,'');assert.equal(p.get('exit-username').value,'');
  assert.match(p.get('exit-auth-note').textContent,/已配置认证/);
  p.get('exit-multiplier').value='2';await p.get('exit-save').events.click();
  const sent=p.actions[1];
  assert.equal(sent.action,'save-exit');assert.equal(sent.exit.id,'exit-test');
  assert.equal(sent.exit.multiplier,2);assert.equal(sent.exit.password,undefined);assert.equal(sent.exit.username,undefined);
  assert.match(p.get('exit-feedback').textContent,/保存失败/);
  assert.equal(p.get('exit-form').classList.contains('hidden'),false);
});

test('new pool is confirmed once and sensitive drafts are cleared after success',async()=>{
  const p=panel();p.get('pool-add').events.click();
  p.get('exit-label').value='聚合出口';p.get('exit-url').value='http://192.0.2.1:8080';
  p.get('exit-kind').value='pool';p.get('exit-attempts').value='100';p.get('exit-multiplier').value='2';
  p.get('exit-username').value='test-user';p.get('exit-password').value='TEST_PASSWORD_CANARY';
  assert.equal(p.actions.length,0);
  p.setResult(true);await p.get('exit-save').events.click();
  assert.equal(p.actions.length,1);assert.equal(p.actions[0].exit.pool,true);
  assert.equal(p.actions[0].exit.attempts,100);assert.equal(p.actions[0].exit.multiplier,2);
  assert.equal(p.get('exit-form').classList.contains('hidden'),true);
  assert.equal(p.get('exit-url').value,'');assert.equal(p.get('exit-password').value,'');
});

test('proxy form validates budgets and never sends retained credentials when clearing auth',async()=>{
  const p=panel();p.openExit({id:'exit-test',proxy:'http://192.0.2.1:8080',has_auth:true});
  for(const invalid of ['','0','-1','101','NaN']){
    p.get('exit-multiplier').value=invalid;await p.get('exit-save').events.click();
    assert.equal(p.actions.length,0);
  }
  p.get('exit-multiplier').value='0.5';p.get('exit-clear-auth').checked=true;
  p.get('exit-username').value='test-user';p.get('exit-password').value='TEST_PASSWORD_CANARY';
  await p.get('exit-save').events.click();
  assert.equal(p.actions[0].exit.clear_auth,true);assert.equal(p.actions[0].exit.password,undefined);
});

test('serial interval draft survives refresh validates bounds and saves once',async()=>{
  const p=panel();p.render(fixture({interval_seconds:2}));
  assert.equal(p.get('probe-interval-seconds').value,'2');
  assert.match(p.get('probe-interval-feedback').textContent,/当前生效：2 秒（配置默认）/);
  p.get('probe-interval-seconds').value='30';p.get('probe-interval-seconds').events.input();
  p.render(fixture({interval_seconds:5}));
  assert.equal(p.get('probe-interval-seconds').value,'30');
  assert.equal(p.actions.length,0);
  for(const invalid of ['','-1','0','3601','1.5','1e2']){
    p.get('probe-interval-seconds').value=invalid;await p.get('probe-interval-save').events.click();
    assert.equal(p.actions.length,0);
  }
  assert.match(p.get('probe-interval-feedback').textContent,/请输入 1–3600 的整数秒/);
  p.setResult(true);
  p.get('probe-interval-seconds').value='3600';await p.get('probe-interval-save').events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.actions)),[{action:'probe-interval',seconds:3600}]);
  assert.match(p.get('probe-interval-feedback').textContent,/已保存：3600 秒；下一次等待起生效/);
});

test('serial interval confirmation stays stable during polling and reports failure',async()=>{
  const p=panel();p.render(fixture());
  let finish;const pending=new Promise(resolve=>{finish=resolve;});p.setResult(pending);
  p.get('probe-interval-seconds').value='45';p.get('probe-interval-seconds').events.input();
  const saving=p.get('probe-interval-save').events.click();
  p.render(fixture({interval_seconds:2,interval_override:false}));
  assert.equal(p.get('probe-interval-seconds').value,'45');
  assert.equal(p.get('probe-interval-save').disabled,true);
  finish(false);await saving;
  assert.match(p.get('probe-interval-feedback').textContent,/保存失败，原设置未更改/);
  assert.equal(p.get('probe-interval-save').disabled,false);
  assert.equal(p.actions[0].action,'probe-interval');
  assert.equal(p.actions.length,1);
  p.setResult(true);
  p.get('probe-interval-save').events.click();
  assert.equal(p.get('probe-interval-seconds').value,'45');
});


test('embedded CPA theme follows white, unmarked paper and dark without OS overriding it',()=>{
  const tokens={'--bg-secondary':'#ffffff','--text-secondary':'#6d6760'};
  const p=panel({embedded:true,hostTheme:'white',darkSystem:true,hostTokens:tokens});
  assert.equal(p.root.getAttribute('data-theme'),'white');
  assert.equal(p.root.style['--bg'],'#ffffff');
  tokens['--bg-secondary']='#faf9f5';p.changeHost('');
  assert.equal(p.root.getAttribute('data-theme'),'light');
  assert.equal(p.root.style['--bg'],'#faf9f5');
  tokens['--bg-secondary']='#151412';p.changeHost('dark');
  assert.equal(p.root.getAttribute('data-theme'),'dark');
  assert.equal(p.root.style['--bg'],'#151412');
  p.changeSystem(false);assert.equal(p.root.getAttribute('data-theme'),'dark');
  delete tokens['--bg-secondary'];p.changeHost('white');
  assert.equal(p.root.style['--bg'],undefined);
});

test('standalone and inaccessible parent use CPA storage with live system fallback',()=>{
  for(const crossOrigin of [false,true]){
    const p=panel({crossOrigin,storedTheme:'white',darkSystem:true});
    assert.equal(p.root.getAttribute('data-theme'),'white');
    p.changeStored('light');assert.equal(p.root.getAttribute('data-theme'),'light');
    p.changeStored('dark');assert.equal(p.root.getAttribute('data-theme'),'dark');
    p.changeSystem(false);assert.equal(p.root.getAttribute('data-theme'),'dark');
    p.changeStored('auto');assert.equal(p.root.getAttribute('data-theme'),'white');
    p.changeSystem(true);assert.equal(p.root.getAttribute('data-theme'),'dark');
  }
});

test('collapsed settings summary shows effective values without replacing open drafts',()=>{
  const p=panel();p.render(fixture({prefetch_minutes:3,interval_seconds:2}));
  p.get('probe-settings').open=true;
  p.get('prefetch-minutes').value='12';p.get('prefetch-minutes').events.input();
  p.render(fixture({prefetch_minutes:5,interval_seconds:8}));
  assert.equal(p.get('probe-settings').open,true);
  assert.equal(p.get('prefetch-minutes').value,'12');
  assert.equal(p.get('settings-summary').textContent,'提前预备 5 分钟 · 串行间隔 8 秒');
  p.render(fixture({settings_error:'设置不可读'}));
  assert.equal(p.get('settings-summary').textContent,'设置异常 · 展开查看');
  assert.equal(p.get('prefetch-save').disabled,true);
});

test('proxy list preserves long details, visible actions and disabled versus cooling counts',async()=>{
  const p=panel();const error='出口连接失败 '.repeat(40);
  p.renderPool({pool_total:3,pool_active:1,pool_disabled:1,proxies_state:[
    {id:'disabled',proxy:'http://192.0.2.1:8080',disabled:true,active:false,last_error:error},
    {id:'cooling',proxy:'http://192.0.2.2:8080',active:false,rest:true,remaining_seconds:300,until:new Date().toISOString()},
    {id:'ready',proxy:'direct',active:true}
  ]});
  assert.equal(p.get('pool-cooling').textContent,'1');
  const first=p.get('pool-rows').children[0];
  assert.match(first.textContent,new RegExp(error));
  const ops=first.children[0].children[3];
  assert.deepEqual(ops.children.map(button=>button.textContent),['编辑','启用','重置']);
  ops.children[0].events.click();assert.equal(p.get('exit-label').focused,true);
  await ops.children[2].events.click();
  assert.equal(p.actions[0].action,'reset-exit');assert.equal(p.actions[0].id,'disabled');
  assert.match(p.get('pool-rows').children[1].textContent,/轮休至.*剩余 5m 00s/);
});
