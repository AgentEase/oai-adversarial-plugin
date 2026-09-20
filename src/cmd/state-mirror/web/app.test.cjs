const {test}=require('node:test');
const assert=require('node:assert/strict');
const {messages,locales,ticketState,isStale,normalizeLocale,shouldApply}=require('./app.js');

test('countdowns expire locally without promoting prepared tickets',()=>{
  const start=Date.parse('2026-01-01T00:00:00Z');
  const ticket={valid:true,issued_at:new Date(start).toISOString(),expires_at:new Date(start+60000).toISOString()};
  assert.deepEqual(ticketState(ticket,start+30000),{kind:'remaining',seconds:30,ratio:0.5});
  assert.equal(ticketState(ticket,start+60000).kind,'expired');
  assert.equal(ticketState({...ticket,valid:false},start).kind,'invalid');
  assert.equal(ticketState({valid:true},start).kind,'noTicket');
  assert.equal(ticketState(null,start).kind,'noTicket');
});

test('live browser connection does not hide a stale plugin snapshot',()=>{
  const now=Date.now();const v={snapshot:{sent_at:new Date(now).toISOString()},received_at:new Date(now).toISOString(),stale_after_seconds:90};
  assert.equal(isStale(v,now+89000),false);assert.equal(isStale(v,now+91000),true);
  v.snapshot.sent_at=new Date(now-100000).toISOString();assert.equal(isStale(v,now),true);
});

test('four-language labels preserve parameters and normalize browser locale',()=>{
  assert.equal(normalizeLocale('zh-Hant-HK'),'zh-TW');assert.equal(normalizeLocale('zh-CN'),'zh-CN');assert.equal(normalizeLocale('ru-RU'),'ru');
  for(const [key,values] of Object.entries(messages)){
    assert.equal(values.length,locales.length,key);
    const tokens=s=>[...s.matchAll(/\{(\w+)\}/g)].map(m=>m[1]).sort();
    for(const value of values)assert.deepEqual(tokens(value),tokens(values[0]),key);
  }
});

test('late HTTP responses cannot overwrite newer SSE data even within the same millisecond',()=>{
  const stamp='2026-01-01T00:00:00.123Z';
  const current={snapshot:{instance:'a',sequence:4,started_at:stamp},received_at:stamp,server_time:stamp};
  assert.equal(shouldApply(current,{...current,snapshot:{...current.snapshot,sequence:3}}),false);
  assert.equal(shouldApply(current,{...current,snapshot:{...current.snapshot,sequence:5}}),true);
  assert.equal(shouldApply(current,{snapshot:null}),false);
  assert.equal(shouldApply(current,{...current,snapshot:{instance:'old',sequence:80,started_at:'2025-12-31T00:00:00Z'}}),false);
});
