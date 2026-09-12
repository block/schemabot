// NODE_PATH must resolve Playwright. Requires Chrome and ImageMagick on PATH.
const fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const {execFileSync}=require('node:child_process');
const {chromium}=require('playwright');
const illustrations={'spirit-ddl-selection':30,'spirit-change-lifecycle':22,'spirit-checkpoint-resume':22};
// Keep the lifecycle motion, but allow four to five seconds to read each short phase.
function frameDelay(name,t){
 if(name!=='spirit-change-lifecycle')return 5;
 return t<6?6:t<8.5?8:t<11.5?8:t<15?7:t<17?10:t<19?10:7;
}
async function render(name){
const root=path.resolve(__dirname,'..'),out=path.join(root,'assets');
const framesDir=fs.mkdtempSync(path.join(os.tmpdir(),name+'-'));
const browser=await chromium.launch({headless:true,...(process.env.CHROME ? {executablePath:process.env.CHROME} : {channel:'chrome'})});
try{
 const page=await browser.newPage({viewport:{width:1100,height:690},deviceScaleFactor:1});
 page.on('pageerror',e=>{throw e});
 await page.setContent(fs.readFileSync(path.join(root,'assets/src/'+name+'.html'),'utf8'));await page.evaluate(()=>document.fonts.ready);
 const frames=[],fps=20,duration=illustrations[name];
 let previous;
 for(let i=0;i<fps*duration;i++){
  const f=path.join(framesDir,String(i).padStart(4,'0')+'.png');
  await page.evaluate(t=>renderFrame(t),i/fps);const png=await page.screenshot();
  const delay=frameDelay(name,i/fps);
  if(previous&&png.equals(previous)){frames[frames.length-1].delay+=delay;}
  else{fs.writeFileSync(f,png);frames.push({file:f,delay});previous=png;}
  if(i%100===0)console.log('Rendered frame '+i+'/'+fps*duration);
 }
 const colors=['ffffff','f6f8fa','d1d9e0','1f2328','59636e','0969da','ddf4ff','1a7f37','dafbe1','9a6700','fff8c5','8250df','fbefff'];
 const swatches=path.join(framesDir,'fixed.ppm'),palette=path.join(framesDir,'palette.png'),adaptive=path.join(framesDir,'adaptive.png');
 fs.writeFileSync(swatches,'P3\n'+colors.length+' 1\n255\n'+colors.map(h=>h.match(/../g).map(v=>parseInt(v,16)).join(' ')).join('\n'));
 execFileSync('magick',[...frames.filter((_,i)=>i%20===0).map(f=>f.file),'-append','-colors',String(256-colors.length),'-unique-colors',adaptive]);
 execFileSync('magick',[adaptive,swatches,'+append','-unique-colors',palette]);
 console.log('Encoding shared-palette GIF');
 execFileSync('magick',['-limit','memory','512MiB','-loop','0',...frames.flatMap(f=>['-delay',String(f.delay),f.file]),'+dither','-remap',palette,'-layers','OptimizePlus',path.join(out,name+'.gif')]);
 console.log('GIF ready: '+fs.statSync(path.join(out,name+'.gif')).size+' bytes');
}finally{await browser.close();fs.rmSync(framesDir,{recursive:true,force:true});}
}
(async()=>{const requested=process.argv.slice(2),names=requested.length?requested:Object.keys(illustrations);
 for(const name of names){if(!Object.hasOwn(illustrations,name))throw new Error('Unknown illustration: '+name);}
 for(const name of names)await render(name);})().catch(e=>{console.error(e);process.exitCode=1});
