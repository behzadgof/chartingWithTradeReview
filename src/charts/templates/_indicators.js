// ============================================================================
// SHARED INDICATOR CALCULATIONS
// Used by both market.html and trades.html templates
// ============================================================================

function ema(arr, period) {
  var k=2/(period+1), result=new Array(arr.length), sum=0, count=0;
  for(var i=0;i<arr.length;i++){
    if(i<period){sum+=arr[i];count++;result[i]=sum/count;}
    else if(i===period){sum+=arr[i];result[i]=sum/(period+1);}
    else{result[i]=arr[i]*k+result[i-1]*(1-k);}
  }
  return result;
}

function sma(arr,period){
  var result=new Array(arr.length);
  for(var i=0;i<arr.length;i++){
    if(i<period-1){result[i]=null;continue;}
    var s=0;for(var j=i-period+1;j<=i;j++)s+=arr[j];
    result[i]=s/period;
  }
  return result;
}

function wma(arr,period){
  var result=new Array(arr.length), denom=period*(period+1)/2;
  for(var i=0;i<arr.length;i++){
    if(arr[i]===null||arr[i]===undefined||i<period-1){result[i]=null;continue;}
    var s=0,valid=true;
    for(var j=0;j<period;j++){var v=arr[i-period+1+j];if(v===null||v===undefined){valid=false;break;}s+=v*(j+1);}
    result[i]=valid?s/denom:null;
  }
  return result;
}

function dema(arr,period){
  var e1=ema(arr,period),e2=ema(e1,period),result=new Array(arr.length);
  for(var i=0;i<arr.length;i++){result[i]=(e1[i]!==undefined&&e2[i]!==undefined)?2*e1[i]-e2[i]:e1[i];}
  return result;
}

function tema(arr,period){
  var e1=ema(arr,period),e2=ema(e1,period),e3=ema(e2,period),result=new Array(arr.length);
  for(var i=0;i<arr.length;i++){result[i]=(e1[i]!==undefined&&e2[i]!==undefined&&e3[i]!==undefined)?3*e1[i]-3*e2[i]+e3[i]:e1[i];}
  return result;
}

function hma(arr,period){
  var half=Math.max(1,Math.round(period/2)),sqrtP=Math.max(1,Math.round(Math.sqrt(period)));
  var w1=wma(arr,half),w2=wma(arr,period),diff=new Array(arr.length);
  for(var i=0;i<arr.length;i++){diff[i]=(w1[i]!==null&&w2[i]!==null)?2*w1[i]-w2[i]:null;}
  return wma(diff,sqrtP);
}

function kama(arr, period) {
  var result = new Array(arr.length);
  var fastSC = 2 / (2 + 1), slowSC = 2 / (30 + 1);
  for (var i = 0; i < period; i++) result[i] = null;
  if (arr.length <= period) return result;
  result[period] = arr[period];
  for (var i = period + 1; i < arr.length; i++) {
    var direction = Math.abs(arr[i] - arr[i - period]);
    var volatility = 0;
    for (var j = i - period + 1; j <= i; j++) volatility += Math.abs(arr[j] - arr[j - 1]);
    var er = volatility === 0 ? 0 : direction / volatility;
    var sc = er * (fastSC - slowSC) + slowSC;
    sc = sc * sc;
    result[i] = result[i - 1] + sc * (arr[i] - result[i - 1]);
  }
  return result;
}

function t3(arr, period, vf) {
  if (vf === undefined) vf = 0.7;
  var e1 = ema(arr, period), e2 = ema(e1, period), e3 = ema(e2, period);
  var e4 = ema(e3, period), e5 = ema(e4, period), e6 = ema(e5, period);
  var c1 = -vf * vf * vf, c2 = 3 * vf * vf + 3 * vf * vf * vf;
  var c3 = -6 * vf * vf - 3 * vf - 3 * vf * vf * vf, c4 = 1 + 3 * vf + vf * vf * vf + 3 * vf * vf;
  var result = new Array(arr.length);
  for (var i = 0; i < arr.length; i++) {
    if (e6[i] !== undefined && e5[i] !== undefined && e4[i] !== undefined && e3[i] !== undefined)
      result[i] = c1 * e6[i] + c2 * e5[i] + c3 * e4[i] + c4 * e3[i];
    else result[i] = null;
  }
  return result;
}

function trima(arr, period) {
  // Triangular MA: SMA of SMA
  var n = Math.ceil((period + 1) / 2);
  var s1 = sma(arr, n);
  return sma(s1, n);
}

function lsma(arr, period) {
  // Least Squares MA (Linear Regression Value)
  var result = new Array(arr.length);
  for (var i = 0; i < arr.length; i++) {
    if (i < period - 1) { result[i] = null; continue; }
    var sx = 0, sy = 0, sxy = 0, sxx = 0;
    for (var j = 0; j < period; j++) {
      var x = j, y = arr[i - period + 1 + j];
      sx += x; sy += y; sxy += x * y; sxx += x * x;
    }
    result[i] = (period * sxy - sx * sy) / (period * sxx - sx * sx) * (period - 1) + (sy - (period * sxy - sx * sy) / (period * sxx - sx * sx) * sx) / period;
  }
  return result;
}

function smma(arr, period) {
  // Smoothed MA (used in RSI, same as RMA/Wilder's MA)
  var result = new Array(arr.length);
  for (var i = 0; i < period - 1; i++) result[i] = null;
  if (arr.length < period) return result;
  var s = 0; for (var i = 0; i < period; i++) s += arr[i];
  result[period - 1] = s / period;
  for (var i = period; i < arr.length; i++) result[i] = (result[i - 1] * (period - 1) + arr[i]) / period;
  return result;
}

// Data source extraction from bars
function getSource(bars, source) {
  switch (source) {
    case 'open':  return bars.map(function(b) { return b.open; });
    case 'high':  return bars.map(function(b) { return b.high; });
    case 'low':   return bars.map(function(b) { return b.low; });
    case 'close': return bars.map(function(b) { return b.close; });
    case 'hl2':   return bars.map(function(b) { return (b.high + b.low) / 2; });
    case 'hlc3':  return bars.map(function(b) { return (b.high + b.low + b.close) / 3; });
    case 'ohlc4': return bars.map(function(b) { return (b.open + b.high + b.low + b.close) / 4; });
    case 'hlcc4': return bars.map(function(b) { return (b.high + b.low + b.close + b.close) / 4; });
    default:      return bars.map(function(b) { return b.close; });
  }
}

var DATA_SOURCES = ['close','open','high','low','hl2','hlc3','ohlc4','hlcc4'];

function calcMA(data, maType, period) {
  switch(maType) {
    case 'SMA':  return sma(data, period);
    case 'EMA':  return ema(data, period);
    case 'WMA':  return wma(data, period);
    case 'DEMA': return dema(data, period);
    case 'TEMA': return tema(data, period);
    case 'HMA':  return hma(data, period);
    case 'KAMA': return kama(data, period);
    case 'T3':   return t3(data, period);
    case 'TRIMA':return trima(data, period);
    case 'LSMA': return lsma(data, period);
    case 'SMMA': return smma(data, period);
    default: return ema(data, period);
  }
}

function calcRSI(bars,period){
  var c=bars.map(function(b){return b.close;}),g=[],l=[];
  for(var i=1;i<c.length;i++){var d=c[i]-c[i-1];g.push(d>0?d:0);l.push(d<0?-d:0);}
  if(g.length<period)return[];
  var ag=0,al=0;for(var i=0;i<period;i++){ag+=g[i];al+=l[i];}ag/=period;al/=period;
  var r=[],rsi=al===0?100:100-100/(1+ag/al);r.push({time:bars[period].time,value:rsi});
  for(var i=period;i<g.length;i++){ag=(ag*(period-1)+g[i])/period;al=(al*(period-1)+l[i])/period;rsi=al===0?100:100-100/(1+ag/al);r.push({time:bars[i+1].time,value:rsi});}
  return r;
}

function calcCCI(bars,period){
  var tp=bars.map(function(b){return(b.high+b.low+b.close)/3;}),r=[];
  for(var i=period-1;i<tp.length;i++){
    var sl=tp.slice(i-period+1,i+1),mn=sl.reduce(function(a,b){return a+b;},0)/period;
    var md=sl.reduce(function(a,b){return a+Math.abs(b-mn);},0)/period;
    r.push({time:bars[i].time,value:md===0?0:(tp[i]-mn)/(0.015*md)});
  }
  return r;
}

function calcADX(bars,period){
  if(bars.length<period+1)return[];
  var trA=[],dpA=[],dmA=[];
  for(var i=1;i<bars.length;i++){
    var hi=bars[i].high-bars[i-1].high,lo=bars[i-1].low-bars[i].low;
    dpA.push(hi>lo&&hi>0?hi:0);dmA.push(lo>hi&&lo>0?lo:0);
    trA.push(Math.max(bars[i].high-bars[i].low,Math.abs(bars[i].high-bars[i-1].close),Math.abs(bars[i].low-bars[i-1].close)));
  }
  var atr=0,sdp=0,sdm=0;for(var i=0;i<period;i++){atr+=trA[i];sdp+=dpA[i];sdm+=dmA[i];}
  var dxA=[],r=[];
  for(var i=period;i<trA.length;i++){
    if(i>period){atr=atr-atr/period+trA[i];sdp=sdp-sdp/period+dpA[i];sdm=sdm-sdm/period+dmA[i];}
    var pdi=atr===0?0:100*sdp/atr,mdi=atr===0?0:100*sdm/atr,dx=(pdi+mdi)===0?0:100*Math.abs(pdi-mdi)/(pdi+mdi);
    dxA.push(dx);
    if(dxA.length>=period){
      var adx;
      if(dxA.length===period)adx=dxA.reduce(function(a,b){return a+b;},0)/period;
      else adx=(r[r.length-1].value*(period-1)+dx)/period;
      r.push({time:bars[i+1].time,value:adx});
    }
  }
  return r;
}

function calcWaveTrend(bars,n1,n2){
  var ap=bars.map(function(b){return(b.high+b.low+b.close)/3;});
  var esaA=ema(ap,n1),diffA=ap.map(function(v,i){return Math.abs(v-(esaA[i]||v));}),dA=ema(diffA,n1);
  var ciA=ap.map(function(v,i){var dd=dA[i]||1;return dd===0?0:(v-(esaA[i]||v))/(0.015*dd);});
  var wt1A=ema(ciA,n2),wt2A=sma(wt1A,4);
  var wt1=[],wt2=[],hist=[],crosses=[];
  for(var i=0;i<bars.length;i++){
    if(wt1A[i]===undefined||isNaN(wt1A[i]))continue;
    wt1.push({time:bars[i].time,value:wt1A[i]});
    if(wt2A[i]!==null&&wt2A[i]!==undefined&&!isNaN(wt2A[i])){
      wt2.push({time:bars[i].time,value:wt2A[i]});
      var d=wt1A[i]-wt2A[i];
      hist.push({time:bars[i].time,value:d,color:d>=0?'rgba(63,185,80,0.35)':'rgba(248,81,73,0.35)'});
    }
    if(i>0&&wt2A[i]!==null&&wt2A[i-1]!==null&&wt1A[i-1]!==undefined&&wt2A[i-1]!==undefined){
      var pD=wt1A[i-1]-wt2A[i-1],cD=wt1A[i]-wt2A[i];
      if((pD<=0&&cD>0)||(pD>=0&&cD<0))crosses.push({time:bars[i].time,position:cD>0?'belowBar':'aboveBar',color:cD>0?'#3fb950':'#f85149',shape:'circle',text:''});
    }
  }
  return{wt1:wt1,wt2:wt2,histogram:hist,crosses:crosses};
}

function calcMACD(bars,fast,slow){
  var c=bars.map(function(b){return b.close;}),ef=ema(c,fast),es=ema(c,slow),ml=[],si=[];
  for(var i=0;i<bars.length;i++){if(ef[i]!==undefined&&es[i]!==undefined){var v=ef[i]-es[i];ml.push({time:bars[i].time,value:v});si.push(v);}}
  var sg=ema(si,9),sl=[],h=[];
  for(var i=0;i<sg.length;i++){if(sg[i]!==undefined&&ml[i]){sl.push({time:ml[i].time,value:sg[i]});var d=ml[i].value-sg[i];h.push({time:ml[i].time,value:d,color:d>=0?'rgba(63,185,80,0.4)':'rgba(248,81,73,0.4)'});}}
  return{macd:ml,signal:sl,histogram:h};
}

function calcStochastic(bars,kP,dP){
  var r=[];
  for(var i=kP-1;i<bars.length;i++){
    var hh=-Infinity,ll=Infinity;
    for(var j=i-kP+1;j<=i;j++){if(bars[j].high>hh)hh=bars[j].high;if(bars[j].low<ll)ll=bars[j].low;}
    r.push({time:bars[i].time,value:hh===ll?50:100*(bars[i].close-ll)/(hh-ll)});
  }
  var dL=[];
  for(var i=dP-1;i<r.length;i++){var s=0;for(var j=i-dP+1;j<=i;j++)s+=r[j].value;dL.push({time:r[i].time,value:s/dP});}
  return{k:r,d:dL};
}

// ============================================================================
// SERVER-SIDE STATE PERSISTENCE
// ============================================================================
// In-memory cache populated at page init by bulk-load from /api/state/all.
// saveToStorage fires async POST (fire-and-forget).
// loadFromStorage reads from cache synchronously (no round-trip).
var __stateCache = {};

function _initStateCache(bulkData) {
  if (bulkData && typeof bulkData === 'object') __stateCache = bulkData;
}

function saveToStorage(key, data) {
  __stateCache[key] = data;
  try {
    fetch('/api/state', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({key: key, value: data})
    }).catch(function(){});
  } catch(e) {}
  if (typeof _fbAutoSync === 'function') _fbAutoSync();
}

function loadFromStorage(key) {
  if (__stateCache.hasOwnProperty(key)) {
    try { return JSON.parse(JSON.stringify(__stateCache[key])); } catch(e) { return __stateCache[key]; }
  }
  return null;
}

function deleteFromStorage(key) {
  delete __stateCache[key];
  try {
    fetch('/api/state/delete', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({key: key})
    }).catch(function(){});
  } catch(e) {}
}

function formatVol(v){
  if(v==null)return'--';
  if(v>=1e9)return(v/1e9).toFixed(1)+'B';
  if(v>=1e6)return(v/1e6).toFixed(1)+'M';
  if(v>=1e3)return(v/1e3).toFixed(1)+'K';
  return v.toString();
}
