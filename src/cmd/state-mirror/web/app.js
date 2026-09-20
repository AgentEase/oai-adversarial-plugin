/* Standalone read-only view. No CPA SDK, management credentials or commands. */
(() => {
  'use strict';
  const messages={
    sleeping:['休眠中','Sleeping','休眠中','Спящий режим'],
    sleepOff:['不休眠','Sleep disabled','不休眠','Сон отключён'],
    sleepHours:['休眠 {start}～{end}（UTC+8）','Sleep {start}–{end} (UTC+8)','休眠 {start}～{end}（UTC+8）','Сон {start}–{end} (UTC+8)'],
    title:['模型状态','Model status','模型狀態','Состояние моделей'],intro:['探测状态与健康基线，实时同步。','Live probe status and healthy baselines.','探測狀態與健康基線，即時同步。','Состояние проверок и актуальные базовые значения.'],
    language:['语言','Language','語言','Язык'],theme:['外观','Appearance','外觀','Оформление'],system:['跟随系统','System','跟隨系統','Системное'],white:['纯白','White','純白','Светлое'],paper:['羊毛纸','Paper','羊毛紙','Бумага'],dark:['暗色','Dark','暗色','Тёмное'],
    probeStatus:['探测状态','Probe status','探測狀態','Состояние проверок'],priority:['优先探测模型','Priority model','優先探測模型','Приоритетная модель'],counts:['探测成功 / 总探测','Successful / total probes','探測成功 / 總探測','Успешные / все проверки'],baselines:['模型健康基线','Model baselines','模型健康基線','Базовые значения моделей'],readonly:['只读镜像','Read-only mirror','唯讀鏡像','Только просмотр'],settings:['探测设置','Probe settings','探測設定','Параметры проверок'],
    model:['模型','Model','模型','Модель'],length:['状态长度','State length','狀態長度','Длина состояния'],source:['来源','Source','來源','Источник'],issued:['签发时间（UTC+8）','Issued at (UTC+8)','簽發時間（UTC+8）','Выдано (UTC+8)'],expires:['到期时间（UTC+8）','Expires at (UTC+8)','到期時間（UTC+8）','Истекает (UTC+8)'],validity:['有效期 / 状态','Validity / status','有效期 / 狀態','Срок / состояние'],
    waiting:['等待首次状态同步','Waiting for the first snapshot','等待首次狀態同步','Ожидание первого снимка'],empty:['暂无模型','No models configured','暫無模型','Модели не настроены'],connecting:['正在连接','Connecting','正在連線','Подключение'],live:['状态已同步','Status synchronized','狀態已同步','Состояние синхронизировано'],reconnecting:['连接中断，正在重连','Disconnected · reconnecting','連線中斷，正在重新連線','Связь потеряна · переподключение'],stale:['同步已中断，数据可能已过时','Updates interrupted · data may be stale','同步已中斷，資料可能已過時','Обновления прерваны · данные могут устареть'],updated:['最后同步：{time}','Last synchronized: {time}','最後同步：{time}','Синхронизация: {time}'],
    footer:['倒计时在本页计算 · 时间为 UTC+8','Countdowns run locally · Times in UTC+8','倒數計時於本頁計算 · 時間為 UTC+8','Отсчёт на этой странице · Время UTC+8'],readOnlyNote:['此页面不提供插件控制功能','This page has no plugin controls','此頁面不提供外掛控制功能','На этой странице нет управления плагином'],region:['模型健康基线，可横向滚动','Model baselines, horizontally scrollable','模型健康基線，可水平捲動','Базовые значения, горизонтальная прокрутка'],stats:['探测统计','Probe statistics','探測統計','Статистика проверок'],
    settingsValue:['提前预备 {minutes} 分钟 · 串行间隔 {seconds} 秒','Prepare {minutes} min early · Interval {seconds} s','提前預備 {minutes} 分鐘 · 串行間隔 {seconds} 秒','Подготовка за {minutes} мин · Интервал {seconds} с'],settingsError:['设置异常','Settings error','設定異常','Ошибка параметров'],
    disabled:['未启用','Disabled','未啟用','Отключено'],error:['配置错误','Configuration error','設定錯誤','Ошибка конфигурации'],unchecked:['不检测','Not checked','不檢測','Не проверяется'],stopping:['正在停止','Stopping','正在停止','Остановка'],running:['运行中','Running','執行中','Выполняется'],halted:['全部停止','All stopped','全部停止','Всё остановлено'],standby:['自动预备待命','Auto-prepare standby','自動預備待命','Ожидание автоподготовки'],manual:['手动待命','Manual standby','手動待命','Ожидание ручного запуска'],offline:['插件已离线','Plugin offline','外掛已離線','Плагин не в сети'],queued:['排队中','Queued','排隊中','В очереди'],probing:['探测中','Probing','探測中','Проверка'],paused:['已暂停','Paused','已暫停','Приостановлено'],unavailable:['探测不可用','Probing unavailable','探測不可用','Проверка недоступна'],
    probe:['探测获取','Probe','探測取得','Проверка'],businessSource:['业务观测','Business observation','業務觀測','Рабочие запросы'],seed:['历史种子','History seed','歷史種子','История'],response:['业务响应','Response','業務回應','Ответ'],stream:['流式响应','Stream','串流回應','Поток'],websocket:['WebSocket','WebSocket','WebSocket','WebSocket'],request:['业务请求','Request','業務請求','Запрос'],config:['配置值','Configured','設定值','Конфигурация'],unknown:['未知','Unknown','未知','Неизвестно'],
    prepared:['预备：{time}','Prepared: {time}','預備：{time}','Резерв: {time}'],remaining:['剩余 {time}','Remaining {time}','剩餘 {time}','Осталось {time}'],ready:['预备就绪（{time}）','Prepared ({time})','預備就緒（{time}）','Резерв готов ({time})'],expired:['已到期','Expired','已到期','Истёк'],candidateExpired:['预备票据已到期','Prepared ticket expired','預備票據已到期','Резервный билет истёк'],invalid:['基线不可用','Baseline unavailable','基線不可用','Базовое значение недоступно'],noTicket:['暂无有效期信息','Validity unavailable','暫無有效期資訊','Нет данных о сроке'],
    business:['业务确认 · 降智','Business-observed degradation','業務確認 · 降智','Ухудшение в рабочих запросах'],failed:['上次探测 {count} 次失败','Last probe: {count} failures','上次探測 {count} 次失敗','Последняя проверка: ошибок {count}'],suspect:['本轮探测 {count} 次失败','Current round: {count} failures','本輪探測 {count} 次失敗','В этом раунде: ошибок {count}'],stillUsable:['现有基线继续使用','Existing baseline remains usable','現有基線繼續使用','Текущее значение остаётся пригодным']
  };
  const locales=['zh-CN','en','zh-TW','ru'];
  function normalizeLocale(value){const v=String(value||'').toLowerCase();return /^(zh-tw|zh-hk|zh-hant)/.test(v)?'zh-TW':v.startsWith('zh')?'zh-CN':v.startsWith('ru')?'ru':'en';}
  function ticketState(ticket,now){
    if(!ticket)return {kind:'noTicket',seconds:0,ratio:0};
    const end=Date.parse(ticket.expires_at),start=Date.parse(ticket.issued_at);
    if(Number.isFinite(end)&&end<=now)return {kind:'expired',seconds:0,ratio:0};
    if(!ticket.valid)return {kind:'invalid',seconds:0,ratio:0};
    if(!Number.isFinite(end))return {kind:'noTicket',seconds:0,ratio:0};
    return {kind:'remaining',seconds:Math.max(0,Math.ceil((end-now)/1000)),ratio:Number.isFinite(start)&&end>start?Math.max(0,Math.min(1,(end-now)/(end-start))):0};
  }
  function isStale(view,now){return !!view?.snapshot&&(now-Date.parse(view.received_at)>view.stale_after_seconds*1000||now-Date.parse(view.snapshot.sent_at)>view.stale_after_seconds*1000);}
  function shouldApply(current,next){
    if(!current?.snapshot)return true;
    if(!next.snapshot)return false;
    if(current.snapshot.instance===next.snapshot.instance){
      if(next.snapshot.sequence!==current.snapshot.sequence)return next.snapshot.sequence>current.snapshot.sequence;
      return Date.parse(next.server_time)>=Date.parse(current.server_time);
    }
    return Date.parse(next.received_at)>=Date.parse(current.received_at)&&Date.parse(next.snapshot.started_at)>=Date.parse(current.snapshot.started_at);
  }
  function duration(seconds){return Math.floor(seconds/60)+'m '+String(seconds%60).padStart(2,'0')+'s';}
  if(typeof module!=='undefined')module.exports={messages,locales,ticketState,isStale,normalizeLocale,shouldApply};
  if(typeof document==='undefined')return;
  const el=id=>document.getElementById(id);
  function stored(key){try{return localStorage.getItem('state-mirror-'+key);}catch{return null;}}
  function save(key,value){try{localStorage.setItem('state-mirror-'+key,value);}catch{}}
  let locale=normalizeLocale(stored('language')||navigator.language),view=null,connected=false,received=false,failed=false;
  let anchorTime=Date.now(),anchorMono=performance.now(),countdowns=[];
  const now=()=>anchorTime+performance.now()-anchorMono;
  const t=(key,params={})=>(messages[key]?.[locales.indexOf(locale)]||key).replace(/\{(\w+)\}/g,(_,name)=>params[name]??'');
  const formatTime=value=>{const date=new Date(value);return Number.isNaN(date.getTime())?'—':new Intl.DateTimeFormat(locale,{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).format(date);};
  function cell(row,text,className=''){const td=document.createElement('td');td.className=className;td.textContent=text;row.append(td);return td;}
  function textNode(parent,text,className='sub'){const div=document.createElement('div');div.className=className;div.textContent=text;parent.append(div);return div;}
  function badge(parent,text,tone=''){return textNode(parent,text,'badge '+tone);}
  function paintLanguage(){
    document.documentElement.lang=locale;document.title=t('title');el('language').value=locale;
    for(const node of document.querySelectorAll('[data-text]'))node.textContent=t(node.dataset.text);
    el('table-region').setAttribute('aria-label',t('region'));document.querySelector('.stats').setAttribute('aria-label',t('stats'));
    render();
  }
  function render(){
    const state=view?.snapshot?.state;
    countdowns=[];
    if(!state){el('status').textContent=el('priority').textContent=el('counts').textContent=el('settings').textContent='—';const tr=document.createElement('tr');cell(tr,t('waiting'),'empty').colSpan=6;el('models').replaceChildren(tr);tick();return;}
    el('status').textContent=t(state.status);el('priority').textContent=state.priority_model||'—';
    el('counts').textContent=state.probes_ok.toLocaleString(locale)+' / '+state.probes_total.toLocaleString(locale);
    const sleepStart=state.sleep_start_hour??0,sleepEnd=state.sleep_end_hour??0;
    el('settings').textContent=t('settingsValue',{minutes:state.prefetch_minutes,seconds:state.interval_seconds})+' · '+(sleepStart===sleepEnd?t('sleepOff'):t('sleepHours',{start:String(sleepStart).padStart(2,'0')+':00',end:String(sleepEnd).padStart(2,'0')+':00'}))+(state.settings_error?' · '+t('settingsError'):'');
    const rows=document.createDocumentFragment();
    for(const model of state.models){
      const tr=document.createElement('tr');const name=cell(tr,'');textNode(name,model.name,'model');
      if(!model.detection){for(let i=0;i<4;i++)cell(tr,'—','muted');badge(cell(tr,''),t('unchecked'));rows.append(tr);continue;}
      textNode(name,t(model.activity));
      const ticket=model.active,length=cell(tr,'');if(ticket?.length)badge(length,ticket.length+' B',ticket.valid?'ok':'warn');else length.textContent='—';
      cell(tr,ticket?t(ticket.source==='business'?'businessSource':ticket.source):'—');
      for(const field of ['issued_at','expires_at']){const td=cell(tr,formatTime(ticket?.[field]),'time');if(model.candidate)textNode(td,t('prepared',{time:formatTime(model.candidate[field])}),'candidate');}
      const ttl=cell(tr,'','ttl');
      const evidence=badge(ttl,'');
      const left=textNode(ttl,'','remaining');
      const progress=document.createElement('progress');progress.className='bar';progress.max=1;progress.setAttribute('aria-label',t('validity'));ttl.append(progress);
      const candidate=model.candidate?textNode(ttl,'','candidate'):null;
      countdowns.push({model,evidence,left,progress,candidate});rows.append(tr);
    }
    if(!state.models.length){const tr=document.createElement('tr');cell(tr,t('empty'),'empty').colSpan=6;rows.append(tr);}
    el('models').replaceChildren(rows);tick();
  }
  function tick(){
    const time=now(),snapshot=view?.snapshot;
    let key=!received?(failed?'reconnecting':'connecting'):snapshot&&isStale(view,time)?'stale':!connected?'reconnecting':snapshot?'live':'waiting';
    if(snapshot?.state.status==='offline')key='offline';
    el('connection').textContent=t(key);el('connection').dataset.tone=key==='live'?'ok':key==='stale'||key==='offline'||key==='reconnecting'?'warn':'';
    el('updated').textContent=snapshot?t('updated',{time:formatTime(view.received_at)}):'';
    for(const row of countdowns){
      const {model,evidence,left,progress,candidate}=row;const active=ticketState(model.active,time);
      const busy=['probing','queued','stopping'].includes(model.activity);
      evidence.hidden=!busy&&model.evidence==='none';
      evidence.textContent=busy?t(model.activity):t(model.evidence,{count:model.failure_attempts});
      evidence.className='badge '+(busy?'ok':model.evidence==='business'?'danger':'warn');
      if(!busy&&['failed','suspect'].includes(model.evidence)&&active.kind==='remaining')evidence.textContent+=' · '+t('stillUsable');
      left.textContent=t(active.kind,{time:duration(active.seconds)});
      left.className='remaining'+(['expired','invalid'].includes(active.kind)?' badge danger':'');
      progress.hidden=active.kind!=='remaining';progress.value=active.ratio;
      if(candidate){
        const pending=ticketState(model.candidate,time);
        candidate.textContent=pending.kind==='expired'?t('candidateExpired'):pending.kind==='remaining'?t('ready',{time:duration(pending.seconds)}):t(pending.kind);
        candidate.className='candidate badge '+(pending.kind==='remaining'?'ok':['expired','invalid'].includes(pending.kind)?'danger':'');
      }
    }
  }
  function applyView(next){
    // Late HTTP/SSE snapshots must not roll back a newer receiver snapshot.
    if(!shouldApply(view,next))return;
    view=next;received=true;anchorTime=Date.parse(next.server_time);anchorMono=performance.now();render();
  }
  const media=matchMedia('(prefers-color-scheme: dark)');
  let theme=stored('theme')||'system';if(!['system','white','paper','dark'].includes(theme))theme='system';
  function paintTheme(){document.documentElement.dataset.theme=theme==='system'?(media.matches?'dark':'white'):theme;el('theme').value=theme;}
  el('theme').addEventListener('change',()=>{theme=el('theme').value;save('theme',theme);paintTheme();});media.addEventListener('change',paintTheme);
  el('language').addEventListener('change',()=>{locale=el('language').value;save('language',locale);paintLanguage();});
  paintTheme();paintLanguage();setInterval(tick,1000);
  async function refresh(){try{const res=await fetch('/state/snapshot',{cache:'no-store',credentials:'omit',signal:AbortSignal.timeout(5000)});if(res.ok)applyView(await res.json());}catch{/* SSE reconnect and stale indicator preserve the last known snapshot. */}}
  refresh().finally(()=>{
    const stream=new EventSource('/state/events');
    stream.addEventListener('state',event=>{try{const next=JSON.parse(event.data);connected=true;applyView(next);}catch{connected=false;tick();}});
    stream.onerror=()=>{connected=false;failed=true;tick();};
  });
  document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
})();
