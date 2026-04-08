import { useState, useRef, useEffect } from "react";

const API = "http://localhost:8000";
const WS  = `ws://${window.location.host}`;

const C = {
  bg:"#05050f",
  bgGrad:"linear-gradient(135deg,#05050f 0%,#0a0a1a 50%,#06060f 100%)",
  card:"rgba(255,255,255,0.04)",card2:"rgba(255,255,255,0.02)",
  cardHover:"rgba(255,255,255,0.07)",border:"rgba(255,255,255,0.08)",
  borderHover:"rgba(124,111,205,0.4)",
  purple:"#7c6fcd",purpleL:"#9d8fe8",green:"#0ea472",greenL:"#10c984",
  yellow:"#d97706",red:"#dc2626",blue:"#2563eb",blueL:"#3b82f6",
  cyan:"#0891b2",cyanL:"#06b6d4",orange:"#ea580c",pink:"#db2777",
  text:"#f1f5f9",textSub:"#94a3b8",muted:"#475569",
  glow:"0 0 30px rgba(124,111,205,0.15)",glowG:"0 0 30px rgba(14,164,114,0.15)",
};

const STEPS = ["Notebook","Import","Schemas","Mapping","Review","Validation","Ingest","Report"];

const api = {
  health:    ()        => fetch(`${API}/api/health`).then(r=>r.json()),
  notebooks: ()        => fetch(`${API}/api/notebooks`).then(r=>r.json()),
  selectNb:  (id,name) => fetch(`${API}/api/notebook/select`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({folder_id:id,folder_name:name})}).then(r=>r.json()),
  upload:    (file)    => { const fd=new FormData(); fd.append("file",file); return fetch(`${API}/api/upload`,{method:"POST",body:fd}).then(r=>r.json()); },
  erd:       (refresh) => fetch(`${API}/api/erd${refresh?"?refresh=true":""}`).then(r=>r.json()),
  mapping:   ()        => fetch(`${API}/api/mapping`,{method:"POST"}).then(r=>r.json()),
  approve:   (m)       => fetch(`${API}/api/mapping/approve`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({mapping:m})}).then(r=>r.json()),
  validate:  ()        => fetch(`${API}/api/validate`,{method:"POST"}).then(r=>r.json()),
  report:    ()        => fetch(`${API}/api/report/latest`).then(r=>r.json()),
};

const DATA_SECTIONS = [
  {
    id:"sample", label:"Sample & Meta", icon:"🧪", color:"#7c6fcd",
    description:"Core sample identifiers and metadata",
    keywords:["sample_id","sample_name","program","target","linker","dar","conjugation","qc_status","cro"],
    defaultSchema:{ id:"ts_bi9do6KL1Z", name:"Sample", type:"Custom Entity" },
    schemaType:"Custom Entity",
  },
  {
    id:"compound", label:"Compound / Molecule", icon:"⚗️", color:"#0891b2",
    description:"Compound structure and chemical properties",
    keywords:["compound","payload","smiles","molecular_weight","hydrophobicity","supplier"],
    defaultSchema:{ id:"ts_bi9do6KL1Z", name:"Sample", type:"Custom Entity" },
    schemaType:"Custom Entity",
  },
  {
    id:"sequence", label:"Sequence & Construct", icon:"🧬", color:"#db2777",
    description:"DNA sequence and construct information",
    keywords:["construct","vector","sequence","gc_content","sequence_length","host_system"],
    defaultSchema:{ id:"ts_JB4gsaH8D4", name:"DNA_Sequence_POC", type:"Custom Entity" },
    schemaType:"Custom Entity",
  },
  {
    id:"batch", label:"Batch Info", icon:"📦", color:"#d97706",
    description:"Batch manufacturing and quality data",
    keywords:["batch","manufacturing","expiry","manufacturer","purity"],
    defaultSchema:{ id:"ts_bi9do6KL1Z", name:"Sample", type:"Custom Entity" },
    schemaType:"Custom Entity",
  },
  {
    id:"inventory", label:"Inventory", icon:"🏪", color:"#0ea472",
    description:"Physical storage, boxes and containers",
    keywords:["storage","box","position","quantity","concentration","location"],
    defaultSchema:{ id:"consch_Gt7eLA5MZd", name:"SV Test Tubes", type:"Container" },
    schemaType:"Container",
  },
  {
    id:"results", label:"Assay Results", icon:"📊", color:"#ea580c",
    description:"Experimental assay results and measurements",
    keywords:["assay_id","assay_type","method","result_value","result_unit","replicate","analyst"],
    defaultSchema:{ id:"assaysch_cETPFdfLCJ", name:"Results-Demo", type:"Assay Result" },
    schemaType:"Assay Result",
  },
  {
    id:"instrument", label:"Instrument Info", icon:"🔬", color:"#65a30d",
    description:"Instrument metadata and calibration records",
    keywords:["instrument_id","instrument_name","instrument_type","manufacturer_inst","model","calibration"],
    defaultSchema:{ id:"assaysch_cETPFdfLCJ", name:"Results-Demo", type:"Assay Result" },
    schemaType:"Assay Result",
  },
];

function getConfidenceExplanation(conf, reason, field, col) {
  if(!col) return {
    label:"No Match",
    color:"#dc2626",
    explanation:`No column in your uploaded file matches the Benchling field "${field}". You need to assign this manually.`,
    how:"The AI searched for exact name matches, partial name overlaps and semantic keyword matches — none were found for this field.",
  };
  if(conf>=99) return {
    label:"Exact Match",
    color:"#0ea472",
    explanation:`The column "${col}" in your file exactly matches the Benchling field "${field}" by name.`,
    how:"Score 99–100: Column name is identical (case-insensitive) to the Benchling field name. No ambiguity.",
  };
  if(conf>=90) return {
    label:"High Confidence",
    color:"#0ea472",
    explanation:`"${col}" is a strong semantic match for "${field}" based on domain knowledge of Benchling schemas.`,
    how:"Score 90–98: Field meaning confirmed by schema context (e.g. 'bases' always maps to DNA sequence data). Very reliable.",
  };
  if(conf>=70) return {
    label:"Partial Match",
    color:"#d97706",
    explanation:`"${col}" partially matches "${field}" — the names overlap but are not identical. Please verify this is correct.`,
    how:"Score 70–89: Keyword overlap found (e.g. 'Construct_Name' contains 'construct'). Usually correct but worth checking.",
  };
  return {
    label:"Low Confidence",
    color:"#d97706",
    explanation:`"${col}" is a weak guess for "${field}". The match was made by fuzzy similarity — please verify or reassign.`,
    how:"Score 1–69: Distant similarity only. Treat as a suggestion, not a confirmed mapping.",
  };
}

function Tooltip({text,children}) {
  const [show,setShow]=useState(false);
  return (
    <div style={{position:"relative",display:"inline-flex"}}
      onMouseEnter={()=>setShow(true)} onMouseLeave={()=>setShow(false)}>
      {children}
      {show&&text&&(
        <div style={{position:"absolute",bottom:"calc(100% + 8px)",left:"50%",
          transform:"translateX(-50%)",background:"#1a1a2e",color:C.text,
          fontSize:11,padding:"8px 12px",borderRadius:7,border:`1px solid ${C.border}`,
          zIndex:300,boxShadow:"0 8px 32px rgba(0,0,0,0.7)",pointerEvents:"none",
          lineHeight:1.6,maxWidth:300,textAlign:"center",whiteSpace:"normal"}}>
          {text}
          <div style={{position:"absolute",top:"100%",left:"50%",transform:"translateX(-50%)",
            borderLeft:"5px solid transparent",borderRight:"5px solid transparent",
            borderTop:`5px solid #1a1a2e`}}/>
        </div>
      )}
    </div>
  );
}

const Tag = ({color,children,onClick,clickable}) => (
  <span onClick={onClick}
    style={{background:color+"20",color,border:`1px solid ${color}35`,
      borderRadius:4,padding:"2px 8px",fontSize:10,fontWeight:600,
      letterSpacing:.5,textTransform:"uppercase",whiteSpace:"nowrap",
      cursor:clickable?"pointer":"default",
      transition:clickable?"all .15s":"none",
      userSelect:"none",
      ...(clickable?{":hover":{background:color+"35"}}:{})
    }}>{children}</span>
);

const Pill = ({color,children,sm}) => (
  <span style={{background:color+"18",color,borderRadius:20,
    padding:sm?"2px 8px":"3px 10px",fontSize:sm?10:11,fontWeight:500,
    border:`1px solid ${color}25`}}>{children}</span>
);

function HCard({children,style={},onClick}) {
  const [hov,setHov]=useState(false);
  return (
    <div onClick={onClick} onMouseEnter={()=>setHov(true)} onMouseLeave={()=>setHov(false)}
      style={{background:hov?C.cardHover:C.card,borderRadius:12,
        border:`1px solid ${hov&&onClick?C.borderHover:C.border}`,padding:16,
        cursor:onClick?"pointer":"default",transition:"all .2s",
        boxShadow:hov&&onClick?C.glow:"none",...style}}>{children}</div>
  );
}

const Btn = ({children,color=C.purple,onClick,disabled,ghost,sm,tooltip,loading}) => {
  const [hov,setHov]=useState(false);
  const btn=(
    <button onClick={onClick} disabled={disabled||loading}
      onMouseEnter={()=>setHov(true)} onMouseLeave={()=>setHov(false)}
      style={{background:ghost?"transparent":(disabled||loading)?"rgba(255,255,255,0.03)":hov?color+"dd":color,
        color:(disabled||loading)?C.muted:ghost?hov?C.purpleL:color:"#fff",
        border:`1px solid ${(disabled||loading)?"rgba(255,255,255,0.06)":color}`,
        borderRadius:7,padding:sm?"5px 14px":"9px 20px",
        cursor:(disabled||loading)?"default":"pointer",fontWeight:600,
        fontSize:sm?11:12,letterSpacing:.3,opacity:(disabled||loading)?.4:1,
        transition:"all .15s",display:"flex",alignItems:"center",gap:6,
        boxShadow:(!disabled&&!ghost&&hov)?`0 0 20px ${color}44`:"none"}}>
      {loading&&<span style={{fontSize:12,animation:"spin .7s linear infinite",display:"inline-block"}}>⟳</span>}
      {children}
    </button>
  );
  return tooltip?<Tooltip text={tooltip}>{btn}</Tooltip>:btn;
};

const InfoBadge = ({text}) => (
  <Tooltip text={text}>
    <span style={{display:"inline-flex",alignItems:"center",justifyContent:"center",
      width:14,height:14,borderRadius:"50%",background:"rgba(255,255,255,0.06)",
      color:C.muted,fontSize:9,cursor:"help",border:`1px solid ${C.border}`,
      fontWeight:700,marginLeft:4,flexShrink:0}}>?</span>
  </Tooltip>
);

const StatBox = ({label,value,color,tooltip}) => (
  <Tooltip text={tooltip}>
    <HCard style={{textAlign:"center",padding:"14px 10px",cursor:"help"}}>
      <div style={{fontSize:26,fontWeight:700,color,lineHeight:1}}>{value}</div>
      <div style={{fontSize:10,color:C.muted,marginTop:4,letterSpacing:.5,textTransform:"uppercase"}}>{label}</div>
    </HCard>
  </Tooltip>
);

const Alert = ({type,msg}) => {
  const c=type==="error"?C.red:type==="success"?C.green:C.yellow;
  return (
    <div style={{background:`${c}10`,border:`1px solid ${c}30`,borderRadius:8,
      padding:"10px 14px",marginBottom:12,fontSize:12,color:C.textSub,
      display:"flex",gap:8,alignItems:"flex-start"}}>
      <span style={{color:c,flexShrink:0}}>{type==="error"?"✕":type==="success"?"✓":"⚠"}</span>
      <span>{msg}</span>
    </div>
  );
};

const Spinner = () => (
  <div style={{display:"inline-block",width:14,height:14,border:`2px solid ${C.border}`,
    borderTopColor:C.purple,borderRadius:"50%",animation:"spin 0.7s linear infinite"}}/>
);

function ConfidenceBar({conf, reason, field, col}) {
  const info = getConfidenceExplanation(conf, reason, field, col);
  return (
    <Tooltip text={
      <div style={{textAlign:"left"}}>
        <div style={{fontWeight:700,color:info.color,marginBottom:4}}>{info.label} · {conf}%</div>
        <div style={{marginBottom:6,color:C.textSub}}>{info.explanation}</div>
        <div style={{fontSize:10,color:C.muted,borderTop:`1px solid ${C.border}`,paddingTop:4}}>
          <strong style={{color:C.muted}}>How scored:</strong> {info.how}
        </div>
      </div>
    }>
      <div style={{display:"flex",alignItems:"center",gap:6,cursor:"help"}}>
        <div style={{width:52,background:"rgba(255,255,255,0.06)",borderRadius:3,height:5}}>
          <div style={{width:`${conf}%`,height:5,borderRadius:3,
            background:conf>=90?C.green:conf>=70?C.yellow:conf>0?C.orange:C.red,
            transition:"width .3s"}}/>
        </div>
        <span style={{color:C.muted,fontSize:10,minWidth:24}}>{conf}%</span>
        <span style={{fontSize:9,color:info.color,fontWeight:600}}>{info.label}</span>
      </div>
    </Tooltip>
  );
}

// ── Step 0: Notebook ──────────────────────────────────────────────────────────
function NotebookStep({onNext}) {
  const [notebooks,setNotebooks]=useState([]);
  const [folders,setFolders]=useState([]);
  const [loading,setLoading]=useState(true);
  const [error,setError]=useState(null);
  const [selected,setSelected]=useState(null);
  const [saving,setSaving]=useState(false);
  const [search,setSearch]=useState("");

  useEffect(()=>{
    api.notebooks()
      .then(data=>{ if(data.error) throw new Error(data.error); setNotebooks(data.notebooks||[]); setFolders(data.folders||[]); })
      .catch(e=>setError(e.message))
      .finally(()=>setLoading(false));
  },[]);

  const handleSelect=async(item)=>{
    setSaving(true);
    try { await api.selectNb(item.id,item.display_name||item.name); setSelected(item); }
    catch(e){ setError(e.message); }
    finally{ setSaving(false); }
  };

  const allItems=[
    ...notebooks.map(n=>({...n,label:"Project"})),
    ...folders.map(f=>({...f,label:"Folder"})),
  ].filter(item=>!search||(item.display_name||item.name).toLowerCase().includes(search.toLowerCase()));

  const typeColor=t=>t==="Project"?C.purple:C.blue;

  return (
    <div>
      <div style={{marginBottom:24}}>
        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
          <div style={{fontSize:11,color:C.purple,letterSpacing:.8,textTransform:"uppercase",fontWeight:600}}>Step 1 of 8</div>
          <InfoBadge text="Select the Benchling folder where your data will be ingested. Only lib_ folders are valid targets."/>
        </div>
        <div style={{fontSize:20,fontWeight:700,color:C.text,marginBottom:4}}>Select Destination Notebook</div>
        <div style={{fontSize:12,color:C.textSub}}>Choose which Benchling folder to ingest your data into.</div>
      </div>
      {error&&<Alert type="error" msg={error}/>}
      {!loading&&(
        <input placeholder="Search folders..." value={search} onChange={e=>setSearch(e.target.value)}
          style={{width:"100%",boxSizing:"border-box",background:C.card2,color:C.text,
            border:`1px solid ${C.border}`,borderRadius:8,padding:"9px 14px",fontSize:12,outline:"none",marginBottom:12}}/>
      )}
      {loading&&<div style={{textAlign:"center",padding:40}}><Spinner/><div style={{color:C.muted,fontSize:12,marginTop:12}}>Fetching folders from Benchling...</div></div>}
      {!loading&&(
        <div style={{display:"flex",flexDirection:"column",gap:6,maxHeight:360,overflowY:"auto",marginBottom:16,paddingRight:4}}>
          {allItems.length===0&&<div style={{textAlign:"center",color:C.muted,fontSize:12,padding:30}}>No folders found.</div>}
          {allItems.map((item,i)=>{
            const isSel=selected?.id===item.id;
            return (
              <div key={i} onClick={()=>handleSelect(item)}
                style={{display:"flex",alignItems:"center",gap:12,
                  background:isSel?`${C.green}12`:C.card2,border:`1px solid ${isSel?C.green:C.border}`,
                  borderRadius:8,padding:"12px 14px",cursor:"pointer",transition:"all .15s",
                  boxShadow:isSel?C.glowG:"none"}}>
                <div style={{width:8,height:8,borderRadius:"50%",flexShrink:0,
                  background:isSel?C.green:typeColor(item.label),boxShadow:isSel?`0 0 8px ${C.green}`:""}}/>
                <div style={{flex:1,minWidth:0}}>
                  <div style={{fontWeight:600,color:C.text,fontSize:12,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>
                    {item.display_name||item.name}
                  </div>
                  <div style={{color:C.muted,fontSize:10,marginTop:1}}>{item.id}</div>
                </div>
                <Tag color={typeColor(item.label)}>{item.label}</Tag>
                {isSel&&<Pill color={C.green} sm>✓ Selected</Pill>}
              </div>
            );
          })}
        </div>
      )}
      {selected&&(
        <div style={{background:`${C.green}08`,border:`1px solid ${C.green}25`,borderRadius:8,
          padding:"10px 14px",marginBottom:14,fontSize:12,color:C.textSub,display:"flex",gap:8}}>
          <span style={{color:C.green}}>✓</span>
          <span>Data will be ingested into <strong style={{color:C.green}}>{selected.display_name||selected.name}</strong>.</span>
        </div>
      )}
      <div style={{display:"flex",justifyContent:"flex-end"}}>
        <Btn onClick={onNext} disabled={!selected||saving} loading={saving}
          tooltip={selected?`Continue with: ${selected.display_name||selected.name}`:"Select a folder first"}>
          Continue →
        </Btn>
      </div>
    </div>
  );
}

// ── Step 1: Import ────────────────────────────────────────────────────────────
function ImportStep({onNext,setUploadData,setErdData}) {
  const [file,setFile]=useState(null);
  const [drag,setDrag]=useState(false);
  const [uploading,setUploading]=useState(false);
  const [analyzing,setAnalyzing]=useState(false);
  const [health,setHealth]=useState(null);
  const [error,setError]=useState(null);
  const [uploadResult,setUploadResult]=useState(null);
  const [erdResult,setErdResult]=useState(null);
  const ref=useRef();

  useEffect(()=>{ api.health().then(setHealth).catch(()=>setHealth({status:"error"})); },[]);

  const handleFile=f=>{ if(f){setFile(f);setUploadResult(null);setError(null);} };
  const handleUpload=async()=>{
    setUploading(true); setError(null);
    try {
      const res=await api.upload(file);
      if(res.error) throw new Error(res.error);
      setUploadResult(res); setUploadData(res);
      setAnalyzing(true);
      const erd=await api.erd(false);
      setErdResult(erd); setErdData(erd);
    } catch(e){ setError(e.message); }
    finally{ setUploading(false); setAnalyzing(false); }
  };

  return (
    <div>
      <div style={{marginBottom:24}}>
        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
          <div style={{fontSize:11,color:C.purple,letterSpacing:.8,textTransform:"uppercase",fontWeight:600}}>Step 2 of 8</div>
          <InfoBadge text="Upload your data file. The system will auto-detect all data sections (samples, results, inventory etc) and match them to Benchling schemas."/>
        </div>
        <div style={{fontSize:20,fontWeight:700,color:C.text,marginBottom:4}}>Import Dataset</div>
        <div style={{fontSize:12,color:C.textSub}}>Upload your data file. All sections will be auto-detected from column names.</div>
      </div>
      {error&&<Alert type="error" msg={error}/>}
      <HCard style={{marginBottom:14,padding:"10px 14px"}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          {health===null&&<Spinner/>}
          {health?.status==="ok"&&<div style={{width:7,height:7,borderRadius:"50%",background:C.green,boxShadow:`0 0 8px ${C.green}`}}/>}
          {health?.status==="error"&&<div style={{width:7,height:7,borderRadius:"50%",background:C.red}}/>}
          <span style={{color:C.text,fontSize:12,fontWeight:500}}>
            {health===null?"Checking...":health?.status==="ok"?"excelra.benchling.com":"Cannot reach backend"}
          </span>
          {health?.status==="ok"&&<Pill color={C.green}>Connected</Pill>}
          {health?.notebook_name&&<Pill color={C.purple}>📁 {health.notebook_name}</Pill>}
          <span style={{color:C.muted,fontSize:11,marginLeft:"auto"}}>
            {health?.benchling_key?"✓ Benchling":"✗ No key"}{" · "}{health?.anthropic_key?"✓ Claude":"✗ Mock mode"}
          </span>
        </div>
      </HCard>
      <div onDragOver={e=>{e.preventDefault();setDrag(true);}} onDragLeave={()=>setDrag(false)}
        onDrop={e=>{e.preventDefault();setDrag(false);handleFile(e.dataTransfer?.files[0]);}}
        onClick={()=>!uploading&&ref.current.click()}
        style={{border:`2px dashed ${drag?C.purple:uploadResult?C.green:C.border}`,borderRadius:12,
          padding:"36px 20px",textAlign:"center",cursor:uploading?"default":"pointer",
          background:drag?`${C.purple}08`:uploadResult?`${C.green}06`:C.card2,
          marginBottom:16,transition:"all .2s",boxShadow:drag?C.glow:uploadResult?C.glowG:"none"}}>
        <input ref={ref} type="file" accept=".xlsx,.csv" style={{display:"none"}} onChange={e=>handleFile(e.target.files[0])}/>
        <div style={{fontSize:30,marginBottom:8}}>{uploadResult?"✓":drag?"⬇":"↑"}</div>
        <div style={{fontWeight:600,color:uploadResult?C.green:drag?C.purple:C.text,fontSize:13,marginBottom:4}}>
          {uploadResult?uploadResult.filename:drag?"Release to upload":"Upload Data File"}
        </div>
        <div style={{color:C.muted,fontSize:11}}>
          {uploadResult?`${uploadResult.rows} rows · ${uploadResult.columns.length} columns detected`:"Accepts .xlsx or .csv · Drag & drop or click to browse"}
        </div>
        {uploadResult&&(
          <div style={{marginTop:10,display:"flex",gap:4,flexWrap:"wrap",justifyContent:"center"}}>
            {uploadResult.columns.slice(0,6).map(c=>(
              <span key={c} style={{background:`${C.green}15`,border:`1px solid ${C.green}30`,borderRadius:4,padding:"2px 7px",fontSize:10,color:C.green}}>{c}</span>
            ))}
            {uploadResult.columns.length>6&&<span style={{fontSize:10,color:C.muted,alignSelf:"center"}}>+{uploadResult.columns.length-6} more</span>}
          </div>
        )}
      </div>
      <div style={{display:"flex",justifyContent:"flex-end",gap:8}}>
        {file&&!uploadResult&&(
          <Btn onClick={handleUpload} loading={uploading||analyzing}>
            {uploading?"Uploading...":analyzing?"Loading schemas...":"🤖 Analyse & Detect Sections →"}
          </Btn>
        )}
        {uploadResult&&erdResult&&(
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",width:"100%"}}>
            <Pill color={C.green}>✓ {uploadResult.columns.length} columns · {uploadResult.rows} rows · sections detected</Pill>
            <Btn onClick={onNext}>Select Schemas →</Btn>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Step 2: Schema Selection ──────────────────────────────────────────────────
function SchemaSelectionStep({onNext,onBack,uploadData,erdData,setSchemaSelections}) {
  const cols = (uploadData?.columns||[]).map(c=>c.toLowerCase().replace(/ /g,"_"));
  const detectedSections = DATA_SECTIONS.filter(section=>
    section.keywords.some(kw => cols.some(c=>c.includes(kw)||kw.includes(c)))
  );
  const schemasByType = {};
  (erdData?.schemas||[]).forEach(s=>{
    if(!schemasByType[s.type]) schemasByType[s.type]=[];
    schemasByType[s.type].push(s);
  });

  const [selections,setSelections]=useState(
    Object.fromEntries(detectedSections.map(s=>[s.id,{mode:"default",schema:s.defaultSchema}]))
  );
  const [confirmed,setConfirmed]=useState({});
  const [expanded,setExpanded]=useState(detectedSections[0]?.id||null);

  const setMode=(sid,mode)=>{
    setSelections(p=>({...p,[sid]:{...p[sid],mode}}));
    setConfirmed(p=>({...p,[sid]:false}));
  };
  const setSchema=(sid,schema)=>{
    setSelections(p=>({...p,[sid]:{...p[sid],schema}}));
    setConfirmed(p=>({...p,[sid]:false}));
  };
  const confirmSection=(sid)=>{
    setConfirmed(p=>({...p,[sid]:true}));
    const idx=detectedSections.findIndex(s=>s.id===sid);
    if(idx<detectedSections.length-1) setExpanded(detectedSections[idx+1].id);
    else setExpanded(null);
  };

  const allConfirmed=detectedSections.every(s=>confirmed[s.id]);
  const confirmedCount=Object.values(confirmed).filter(Boolean).length;
  const typeColor=t=>t==="Custom Entity"?C.blue:t==="Assay Result"?C.cyan:t==="Container"?C.yellow:C.purple;
  const handleNext=()=>{ setSchemaSelections(selections); onNext(); };

  return (
    <div>
      <div style={{marginBottom:20}}>
        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
          <div style={{fontSize:11,color:C.purple,letterSpacing:.8,textTransform:"uppercase",fontWeight:600}}>Step 3 of 8</div>
          <InfoBadge text="The system detected these data sections from your file's column names. For each section, choose Default (standard Benchling schema) or Custom (pick from your tenant's schemas)."/>
        </div>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
          <div>
            <div style={{fontSize:20,fontWeight:700,color:C.text,marginBottom:4}}>Schema Selection</div>
            <div style={{fontSize:12,color:C.textSub}}>
              <strong style={{color:C.text}}>{detectedSections.length} data sections</strong> detected in your file. Confirm the Benchling schema for each.
            </div>
          </div>
          <Pill color={confirmedCount===detectedSections.length?C.green:C.yellow}>
            {confirmedCount}/{detectedSections.length} confirmed
          </Pill>
        </div>
      </div>
      <div style={{background:"rgba(255,255,255,0.06)",borderRadius:6,height:4,marginBottom:20}}>
        <div style={{width:`${detectedSections.length>0?(confirmedCount/detectedSections.length)*100:0}%`,
          height:4,borderRadius:6,background:`linear-gradient(90deg,${C.purple},${C.green})`,transition:"width .4s"}}/>
      </div>
      <div style={{display:"flex",flexDirection:"column",gap:8,marginBottom:20}}>
        {detectedSections.map(section=>{
          const sel=selections[section.id]||{mode:"default",schema:section.defaultSchema};
          const isConf=confirmed[section.id];
          const isOpen=expanded===section.id;
          const availableSchemas=schemasByType[section.schemaType]||[];
          const detectedCols=(uploadData?.columns||[]).filter(c=>
            section.keywords.some(kw=>c.toLowerCase().replace(/ /g,"_").includes(kw)||kw.includes(c.toLowerCase()))
          );
          return (
            <div key={section.id}
              style={{borderRadius:12,border:`1px solid ${isConf?section.color+"50":C.border}`,
                background:isConf?section.color+"06":C.card,overflow:"hidden",transition:"all .2s"}}>
              <div onClick={()=>!isConf&&setExpanded(isOpen?null:section.id)}
                style={{display:"flex",alignItems:"center",gap:12,padding:"14px 16px",cursor:isConf?"default":"pointer"}}>
                <div style={{width:36,height:36,borderRadius:8,background:section.color+"22",
                  border:`1px solid ${section.color}44`,display:"flex",alignItems:"center",
                  justifyContent:"center",fontSize:18,flexShrink:0}}>{section.icon}</div>
                <div style={{flex:1,minWidth:0}}>
                  <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:2}}>
                    <span style={{fontWeight:600,color:C.text,fontSize:13}}>{section.label}</span>
                    <Tag color={section.color}>{detectedCols.length} columns</Tag>
                    {isConf&&<Pill color={C.green} sm>✓ Confirmed</Pill>}
                  </div>
                  <div style={{fontSize:11,color:C.muted}}>{section.description}</div>
                </div>
                <div style={{textAlign:"right",flexShrink:0,marginRight:8}}>
                  <div style={{fontSize:11,fontWeight:500,color:isConf?C.green:C.text,marginBottom:2}}>
                    {sel.schema.name}
                  </div>
                  <Tag color={typeColor(sel.schema.type)}>{sel.schema.type.split(" ")[0]}</Tag>
                </div>
                {!isConf&&<span style={{color:C.muted,fontSize:12}}>{isOpen?"▲":"▼"}</span>}
              </div>
              {isOpen&&!isConf&&(
                <div style={{borderTop:`1px solid ${C.border}`,padding:16}}>
                  <div style={{marginBottom:14}}>
                    <div style={{fontSize:10,color:C.muted,letterSpacing:.5,textTransform:"uppercase",marginBottom:8,fontWeight:600}}>
                      Columns detected in your file for this section
                    </div>
                    <div style={{display:"flex",gap:4,flexWrap:"wrap"}}>
                      {detectedCols.map(c=>(
                        <span key={c} style={{background:section.color+"18",border:`1px solid ${section.color}30`,
                          borderRadius:4,padding:"3px 8px",fontSize:10,color:section.color}}>{c}</span>
                      ))}
                      {detectedCols.length===0&&<span style={{color:C.muted,fontSize:11}}>No columns matched for this section</span>}
                    </div>
                  </div>
                  <div style={{marginBottom:14}}>
                    <div style={{fontSize:10,color:C.muted,letterSpacing:.5,textTransform:"uppercase",marginBottom:8,fontWeight:600}}>
                      Schema Mode
                    </div>
                    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
                      <button onClick={()=>{ setMode(section.id,"default"); setSchema(section.id,section.defaultSchema); }}
                        style={{background:sel.mode==="default"?`${C.purple}22`:"transparent",
                          border:`1px solid ${sel.mode==="default"?C.purple:C.border}`,
                          borderRadius:8,padding:"12px 14px",cursor:"pointer",
                          color:sel.mode==="default"?C.purple:C.muted,transition:"all .15s",textAlign:"left"}}>
                        <div style={{fontWeight:600,fontSize:12,marginBottom:3}}>⚡ Default</div>
                        <div style={{fontSize:10,color:C.muted,lineHeight:1.4}}>
                          Use standard Benchling schema<br/>
                          <span style={{color:C.textSub}}>{section.defaultSchema.name}</span>
                        </div>
                      </button>
                      <button onClick={()=>setMode(section.id,"custom")}
                        style={{background:sel.mode==="custom"?`${C.cyan}22`:"transparent",
                          border:`1px solid ${sel.mode==="custom"?C.cyan:C.border}`,
                          borderRadius:8,padding:"12px 14px",cursor:"pointer",
                          color:sel.mode==="custom"?C.cyan:C.muted,transition:"all .15s",textAlign:"left"}}>
                        <div style={{fontWeight:600,fontSize:12,marginBottom:3}}>🔧 Custom</div>
                        <div style={{fontSize:10,color:C.muted,lineHeight:1.4}}>
                          Choose from your tenant's schemas<br/>
                          <span style={{color:C.textSub}}>{availableSchemas.length} available</span>
                        </div>
                      </button>
                    </div>
                  </div>
                  {sel.mode==="custom"&&(
                    <div style={{marginBottom:14}}>
                      <div style={{fontSize:10,color:C.muted,letterSpacing:.5,textTransform:"uppercase",marginBottom:8,fontWeight:600}}>
                        Select Schema — {section.schemaType} schemas in your Benchling tenant
                      </div>
                      <div style={{display:"flex",flexDirection:"column",gap:5,maxHeight:200,overflowY:"auto"}}>
                        {availableSchemas.length===0&&(
                          <div style={{color:C.muted,fontSize:11,padding:8}}>
                            No {section.schemaType} schemas found in ERD. Try refreshing.
                          </div>
                        )}
                        {availableSchemas.map(schema=>{
                          const isSel=sel.schema.id===schema.id;
                          return (
                            <div key={schema.id} onClick={()=>setSchema(section.id,{id:schema.id,name:schema.name,type:schema.type})}
                              style={{display:"flex",alignItems:"center",gap:10,
                                background:isSel?`${C.cyan}12`:C.card2,
                                border:`1px solid ${isSel?C.cyan:C.border}`,
                                borderRadius:8,padding:"10px 12px",cursor:"pointer",transition:"all .15s"}}>
                              <div style={{width:6,height:6,borderRadius:"50%",flexShrink:0,background:isSel?C.cyan:C.muted}}/>
                              <div style={{flex:1,minWidth:0}}>
                                <div style={{fontWeight:500,color:C.text,fontSize:12}}>{schema.name}</div>
                                <div style={{color:C.muted,fontSize:9,fontFamily:"monospace",marginTop:1}}>{schema.id}</div>
                              </div>
                              <div style={{fontSize:10,color:C.muted}}>{schema.field_count} fields</div>
                              <Tag color={typeColor(schema.type)}>{schema.type.split(" ")[0]}</Tag>
                              {isSel&&<Pill color={C.cyan} sm>Selected</Pill>}
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  )}
                  {sel.mode==="default"&&(
                    <div style={{background:C.card2,borderRadius:8,padding:"10px 12px",marginBottom:14,border:`1px solid ${C.border}`}}>
                      <div style={{display:"flex",alignItems:"center",gap:10}}>
                        <div style={{width:6,height:6,borderRadius:"50%",background:C.green}}/>
                        <div style={{flex:1}}>
                          <div style={{fontWeight:500,color:C.text,fontSize:12}}>{sel.schema.name}</div>
                          <div style={{color:C.muted,fontSize:10,fontFamily:"monospace"}}>{sel.schema.id}</div>
                        </div>
                        <Tag color={typeColor(sel.schema.type)}>{sel.schema.type.split(" ")[0]}</Tag>
                      </div>
                    </div>
                  )}
                  <div style={{display:"flex",justifyContent:"flex-end"}}>
                    <Btn sm color={C.green} onClick={()=>confirmSection(section.id)}
                      tooltip="Confirm this schema selection and move to the next section">
                      Confirm Schema ✓
                    </Btn>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
      <div style={{display:"flex",justifyContent:"space-between"}}>
        <Btn ghost onClick={onBack}>← Back</Btn>
        <Btn onClick={handleNext} disabled={!allConfirmed}
          tooltip={allConfirmed?"All schemas confirmed — proceed to column mapping":"Confirm all sections first"}>
          {allConfirmed?"Proceed to Mapping →":"Confirm all sections"}
        </Btn>
      </div>
    </div>
  );
}

// ── Step 3: Mapping ───────────────────────────────────────────────────────────
function MappingStep({onNext,onBack,uploadData,setMappingData}) {
  const [mapping,setMapping]=useState(null);
  const [loading,setLoading]=useState(true);
  const [error,setError]=useState(null);
  const [active,setActive]=useState(null);
  const [showComments,setShowComments]=useState(false);

  useEffect(()=>{
    api.mapping()
      .then(res=>{
        if(res.error) throw new Error(res.error);
        setMapping(res.mapping);
        setMappingData(res.mapping);
        setActive(Object.keys(res.mapping)[0]);
      })
      .catch(e=>setError(e.message))
      .finally(()=>setLoading(false));
  },[]);

  // ── Toggle status between "auto" and "review" for a specific row ──
  const toggleStatus=(schema,idx)=>{
    setMapping(prev=>{
      const u={...prev};
      u[schema]=[...prev[schema]];
      const current=u[schema][idx];
      const currentStatus=current.status;
      // Only allow toggling between auto/system ↔ review
      const newStatus=(currentStatus==="auto"||currentStatus==="system")?"review":"auto";
      u[schema][idx]={...current,status:newStatus};
      return u;
    });
    // Keep mappingData in sync
    setMappingData(prev=>{
      if(!prev) return prev;
      const u={...prev};
      u[schema]=[...prev[schema]];
      const current=u[schema][idx];
      const newStatus=(current.status==="auto"||current.status==="system")?"review":"auto";
      u[schema][idx]={...current,status:newStatus};
      return u;
    });
  };

  if(loading) return (
    <div style={{textAlign:"center",padding:60}}>
      <Spinner/>
      <div style={{color:C.muted,fontSize:12,marginTop:12}}>Analyzing your dataset against selected schemas...</div>
    </div>
  );
  if(error) return <Alert type="error" msg={`Mapping failed: ${error}`}/>;

  const flat=Object.values(mapping).flat();
  const auto=flat.filter(m=>m.status==="auto"||m.status==="system").length;
  const review=flat.filter(m=>m.status==="review").length;
  const missing=flat.filter(m=>m.status==="missing").length;

  const sColor=s=>s==="auto"?C.green:s==="system"?C.cyan:s==="review"?C.yellow:C.red;
  const sLabel=s=>s==="auto"?"Auto":s==="system"?"System":s==="review"?"Review":"Missing";

  // Only auto/system rows can be toggled (missing stays as-is)
  const isToggleable=s=>s==="auto"||s==="system"||s==="review";

  const health=schema=>{
    const m=mapping[schema];
    if(!m) return C.muted;
    if(m.filter(f=>f.status==="missing").length>0) return C.red;
    if(m.filter(f=>f.status==="review").length>0) return C.yellow;
    return C.green;
  };

  return (
    <div>
      <div style={{marginBottom:20}}>
        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
          <div style={{fontSize:11,color:C.purple,letterSpacing:.8,textTransform:"uppercase",fontWeight:600}}>Step 4 of 8</div>
          <InfoBadge text="Claude AI matched your file columns to Benchling schema fields. Hover over confidence bars for scoring details. Click a status badge to toggle between Auto and Review."/>
        </div>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
          <div>
            <div style={{fontSize:20,fontWeight:700,color:C.text,marginBottom:4}}>AI Schema Mapping</div>
            <div style={{fontSize:12,color:C.textSub}}>Matched {uploadData?.columns?.length||0} columns to Benchling schema fields.</div>
          </div>
          <button onClick={()=>setShowComments(p=>!p)}
            style={{background:showComments?`${C.cyan}22`:"transparent",
              border:`1px solid ${showComments?C.cyan:C.border}`,
              borderRadius:7,padding:"6px 14px",cursor:"pointer",
              color:showComments?C.cyan:C.muted,fontSize:11,fontWeight:600,transition:"all .15s"}}>
            {showComments?"Hide":"Show"} Comments
          </button>
        </div>
      </div>

      {/* Hint banner */}
      <div style={{background:`${C.purple}08`,border:`1px solid ${C.purple}20`,borderRadius:8,
        padding:"8px 12px",marginBottom:14,fontSize:11,color:C.muted,display:"flex",gap:8,alignItems:"center"}}>
        <span style={{color:C.purple}}>💡</span>
        <span>Click any <strong style={{color:C.green}}>Auto</strong> or <strong style={{color:C.yellow}}>Review</strong> status badge to toggle it — force a field to Review if you want to double-check it in the next step.</span>
      </div>

      {showComments&&(
        <HCard style={{marginBottom:16,background:`${C.cyan}06`,border:`1px solid ${C.cyan}25`}}>
          <div style={{fontSize:10,color:C.cyan,letterSpacing:.5,textTransform:"uppercase",marginBottom:10,fontWeight:600}}>
            How Confidence Scores Work
          </div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
            {[
              {range:"99–100%",label:"Exact Match",  color:C.green,  desc:"Column name is identical to Benchling field name (case-insensitive)"},
              {range:"90–98%", label:"High Confidence",color:C.green, desc:"Strong semantic match confirmed by Benchling domain knowledge"},
              {range:"70–89%", label:"Partial Match", color:C.yellow, desc:"Keyword overlap found — names partially match. Review recommended"},
              {range:"1–69%",  label:"Low Confidence",color:C.orange, desc:"Weak fuzzy similarity only. Treat as a suggestion"},
              {range:"0%",     label:"No Match",      color:C.red,    desc:"No column found — must be assigned manually in the Review step"},
            ].map(s=>(
              <div key={s.range} style={{display:"flex",gap:8,alignItems:"flex-start",
                background:C.card2,borderRadius:6,padding:"8px 10px"}}>
                <div style={{minWidth:44,fontSize:10,fontWeight:700,color:s.color,flexShrink:0}}>{s.range}</div>
                <div>
                  <div style={{fontSize:11,fontWeight:600,color:s.color,marginBottom:1}}>{s.label}</div>
                  <div style={{fontSize:10,color:C.muted,lineHeight:1.4}}>{s.desc}</div>
                </div>
              </div>
            ))}
          </div>
        </HCard>
      )}

      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:8,marginBottom:16}}>
        <StatBox label="Total Fields" value={flat.length} color={C.blue}   tooltip="Total Benchling fields across all selected schemas"/>
        <StatBox label="Auto-Mapped"  value={auto}        color={C.green}  tooltip="Matched with ≥90% confidence — no action needed"/>
        <StatBox label="Needs Review" value={review}      color={C.yellow} tooltip="Partial matches or manually flagged — verify before confirming"/>
        <StatBox label="Unresolved"   value={missing}     color={C.red}    tooltip="No match found — assign manually in the next step"/>
      </div>

      <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
        {Object.keys(mapping).map(s=>(
          <button key={s} onClick={()=>setActive(s)} style={{
            background:active===s?health(s)+"22":"transparent",
            border:`1px solid ${active===s?health(s):C.border}`,
            color:active===s?health(s):C.muted,
            borderRadius:7,padding:"6px 14px",cursor:"pointer",fontSize:11,fontWeight:600,transition:"all .15s"}}>{s}</button>
        ))}
      </div>

      {active&&mapping[active]&&(
        <HCard style={{marginBottom:16}}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:14}}>
            <span style={{fontWeight:600,color:C.text,fontSize:13}}>{active}</span>
            <Pill color={C.green}>{mapping[active].filter(m=>m.status==="auto"||m.status==="system").length} auto-mapped</Pill>
          </div>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead>
              <tr style={{color:C.muted,borderBottom:`1px solid ${C.border}`}}>
                {["Benchling Field","Your Column","Confidence & Score Reason",
                  ...(showComments?["Mapping Comment"]:[]),
                  "Status"].map(h=>(
                  <th key={h} style={{padding:"6px 8px",textAlign:"left",fontWeight:500,
                    ...(h==="Status"?{whiteSpace:"nowrap"}:{})}}>{h}
                    {h==="Status"&&<InfoBadge text="Click Auto or Review badges to toggle the status manually"/>}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {mapping[active].map((m,i)=>{
                const field=m.benchling_field||m.field;
                const col=m.suggested_column||m.mapped||null;
                const conf=m.confidence||m.conf||0;
                const info=getConfidenceExplanation(conf,m.reason,field,col);
                const canToggle=isToggleable(m.status);
                return (
                  <tr key={i} style={{borderBottom:`1px solid ${C.border}33`,transition:"background .15s"}}
                    onMouseEnter={e=>e.currentTarget.style.background="rgba(255,255,255,0.02)"}
                    onMouseLeave={e=>e.currentTarget.style.background="transparent"}>
                    <td style={{padding:"8px"}}>
                      <code style={{color:C.cyan,fontSize:10}}>{field}</code>
                      {m.benchling_required&&<span style={{marginLeft:4}}><Tag color={C.red}>req</Tag></span>}
                    </td>
                    <td style={{padding:"8px"}}>
                      <code style={{color:col?C.text:C.muted,fontSize:10}}>{col||"—"}</code>
                    </td>
                    <td style={{padding:"8px",minWidth:160}}>
                      <ConfidenceBar conf={conf} reason={m.reason} field={field} col={col}/>
                    </td>
                    {showComments&&(
                      <td style={{padding:"8px",maxWidth:200}}>
                        <div style={{fontSize:10,color:C.muted,lineHeight:1.4}}>{m.reason}</div>
                        {conf>0&&col&&(
                          <div style={{fontSize:9,color:info.color,marginTop:2,fontWeight:500}}>{info.label}</div>
                        )}
                      </td>
                    )}
                    <td style={{padding:"8px"}}>
                      {canToggle?(
                        <Tooltip text={
                          m.status==="review"
                            ? "Click to mark as Auto (trusted)"
                            : "Click to flag for Review in the next step"
                        }>
                          {/* Clickable tag for toggleable statuses */}
                          <span
                            onClick={()=>toggleStatus(active,i)}
                            style={{
                              background:sColor(m.status)+"20",
                              color:sColor(m.status),
                              border:`1px solid ${sColor(m.status)}35`,
                              borderRadius:4,padding:"2px 8px",fontSize:10,fontWeight:600,
                              letterSpacing:.5,textTransform:"uppercase",whiteSpace:"nowrap",
                              cursor:"pointer",userSelect:"none",
                              display:"inline-flex",alignItems:"center",gap:5,
                              transition:"all .15s",
                            }}
                            onMouseEnter={e=>{e.currentTarget.style.background=sColor(m.status)+"38";}}
                            onMouseLeave={e=>{e.currentTarget.style.background=sColor(m.status)+"20";}}
                          >
                            {sLabel(m.status)}
                            <span style={{fontSize:8,opacity:.7}}>⇄</span>
                          </span>
                        </Tooltip>
                      ):(
                        <Tooltip text={m.reason}>
                          <span><Tag color={sColor(m.status)}>{sLabel(m.status)}</Tag></span>
                        </Tooltip>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </HCard>
      )}

      <div style={{display:"flex",justifyContent:"space-between"}}>
        <Btn ghost onClick={onBack}>← Back</Btn>
        <Btn onClick={onNext}>Review & Confirm →</Btn>
      </div>
    </div>
  );
}

// ── Step 4: Review ────────────────────────────────────────────────────────────
function ReviewStep({onNext,onBack,mappingData,uploadData,setApprovedMapping}) {
  const [mapping,setMapping]=useState(mappingData||{});
  const [confirmed,setConfirmed]=useState({});
  const [ignored,setIgnored]=useState({}); // { "schema|||idx": true }
  const [saving,setSaving]=useState(false);
  const [exported,setExported]=useState(false);
  const [error,setError]=useState(null);
  const cols=uploadData?.columns||[];

  const ignoreKey=(schema,idx)=>`${schema}|||${idx}`;

  const toggleIgnore=(schema,idx)=>{
    const key=ignoreKey(schema,idx);
    setIgnored(p=>({...p,[key]:!p[key]}));
    setConfirmed(p=>({...p,[schema]:false}));
  };

  const update=(schema,idx,col)=>{
    setMapping(prev=>{
      const u={...prev}; u[schema]=[...prev[schema]];
      u[schema][idx]={...u[schema][idx],suggested_column:col,mapped:col,conf:95,confidence:95,reason:"Manually assigned",status:"manual"};
      return u;
    });
    setConfirmed(p=>({...p,[schema]:false}));
    // Un-ignore if the user picks a column
    setIgnored(p=>({...p,[ignoreKey(schema,idx)]:false}));
  };

  // A field "needs attention" if it's review/missing AND not ignored
  const flagged=Object.entries(mapping).flatMap(([schema,fields])=>
    (fields||[])
      .map((f,idx)=>({schema,idx,globalIdx:idx,...f}))
      .filter(f=>(f.status==="review"||f.status==="missing")&&!ignored[ignoreKey(f.schema,f.idx)])
  );

  // Schema is OK to confirm if all its review/missing fields are either resolved or ignored
  const schemaOk=(schema)=>{
    const fields=mapping[schema]||[];
    return fields.every((f,idx)=>
      f.status!=="missing"&&f.status!=="review" ||
      ignored[ignoreKey(schema,idx)]
    );
  };

  const allOk=Object.keys(mapping).length>0&&Object.keys(mapping).every(s=>confirmed[s]);

  // Build final mapping to approve: mark ignored fields explicitly
  const buildApproveMapping=()=>{
    const result={};
    Object.entries(mapping).forEach(([schema,fields])=>{
      result[schema]=(fields||[]).map((f,idx)=>{
        if(ignored[ignoreKey(schema,idx)]) return {...f,status:"ignored",suggested_column:null,mapped:null};
        return f;
      });
    });
    return result;
  };

  const handleNext=async()=>{
    setSaving(true);
    try{
      const approveMapping=buildApproveMapping();
      await api.approve(approveMapping);
      setApprovedMapping(approveMapping);
      onNext();
    }
    catch(e){ setError("Failed to save: "+e.message); }
    finally{ setSaving(false); }
  };

  // Count ignored per schema
  const ignoredCount=(schema)=>
    (mapping[schema]||[]).filter((_,idx)=>ignored[ignoreKey(schema,idx)]).length;

  return (
    <div>
      <div style={{marginBottom:20}}>
        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
          <div style={{fontSize:11,color:C.purple,letterSpacing:.8,textTransform:"uppercase",fontWeight:600}}>Step 5 of 8</div>
          <InfoBadge text="Resolve flagged fields by assigning a column, or ignore them to skip during ingestion. Confirm each schema to proceed."/>
        </div>
        <div style={{fontSize:20,fontWeight:700,color:C.text,marginBottom:4}}>Review & Confirm</div>
        <div style={{fontSize:12,color:C.textSub}}>Assign unresolved fields or ignore them, then confirm each schema.</div>
      </div>
      {error&&<Alert type="error" msg={error}/>}

      {/* Flagged fields panel */}
      {(flagged.length>0||Object.values(ignored).some(Boolean))&&(
        <HCard style={{marginBottom:14,border:`1px solid ${C.yellow}30`,background:`${C.yellow}05`}}>
          <div style={{fontSize:10,color:C.yellow,letterSpacing:.5,textTransform:"uppercase",marginBottom:12,fontWeight:600,display:"flex",justifyContent:"space-between",alignItems:"center"}}>
            <span>{flagged.length} Field{flagged.length!==1?"s":""} Require Attention</span>
            {Object.values(ignored).filter(Boolean).length>0&&(
              <Pill color={C.muted} sm>
                {Object.values(ignored).filter(Boolean).length} ignored
              </Pill>
            )}
          </div>

          {/* Active flagged fields */}
          {flagged.map((f,i)=>(
            <div key={`${f.schema}-${f.idx}`}
              style={{display:"flex",alignItems:"center",gap:10,
                padding:"9px 0",borderBottom:`1px solid ${C.border}44`,flexWrap:"wrap"}}>
              <Tag color={C.blue}>{f.schema}</Tag>
              <code style={{color:C.cyan,fontSize:10,minWidth:130}}>{f.benchling_field||f.field}</code>
              <select onChange={e=>update(f.schema,f.idx,e.target.value)}
                style={{background:"rgba(255,255,255,0.04)",color:C.text,border:`1px solid ${C.yellow}40`,
                  borderRadius:6,padding:"5px 10px",fontSize:11,flex:1,minWidth:160,outline:"none"}}>
                <option value="">— Select column —</option>
                {cols.map(c=><option key={c} value={c}>{c}</option>)}
              </select>
              <span style={{color:C.muted,fontSize:10,maxWidth:140,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{f.reason}</span>
              {/* Ignore button */}
              <Tooltip text="Skip this field — it will be excluded from ingestion">
                <button
                  onClick={()=>toggleIgnore(f.schema,f.idx)}
                  style={{background:"transparent",border:`1px solid ${C.muted}40`,
                    borderRadius:6,padding:"4px 10px",cursor:"pointer",color:C.muted,
                    fontSize:10,fontWeight:600,letterSpacing:.3,transition:"all .15s",
                    display:"flex",alignItems:"center",gap:4,whiteSpace:"nowrap"}}
                  onMouseEnter={e=>{e.currentTarget.style.borderColor=C.orange;e.currentTarget.style.color=C.orange;}}
                  onMouseLeave={e=>{e.currentTarget.style.borderColor=C.muted+"40";e.currentTarget.style.color=C.muted;}}
                >
                  ✕ Ignore
                </button>
              </Tooltip>
            </div>
          ))}

          {/* Ignored fields (collapsed / summary) */}
          {Object.entries(mapping).flatMap(([schema,fields])=>
            (fields||[])
              .map((f,idx)=>({schema,idx,...f}))
              .filter(f=>ignored[ignoreKey(f.schema,f.idx)])
          ).map((f,i,arr)=>(
            <div key={`ign-${f.schema}-${f.idx}`}
              style={{display:"flex",alignItems:"center",gap:10,
                padding:"7px 0",borderBottom:i<arr.length-1?`1px solid ${C.border}22`:"none",
                opacity:.5}}>
              <Tag color={C.muted}>{f.schema}</Tag>
              <code style={{color:C.muted,fontSize:10,flex:1}}>{f.benchling_field||f.field}</code>
              <Pill color={C.muted} sm>Ignored</Pill>
              <Tooltip text="Un-ignore this field">
                <button
                  onClick={()=>toggleIgnore(f.schema,f.idx)}
                  style={{background:"transparent",border:`1px solid ${C.muted}30`,
                    borderRadius:6,padding:"3px 8px",cursor:"pointer",color:C.muted,
                    fontSize:10,fontWeight:600,transition:"all .15s"}}
                  onMouseEnter={e=>{e.currentTarget.style.color=C.green;e.currentTarget.style.borderColor=C.green;}}
                  onMouseLeave={e=>{e.currentTarget.style.color=C.muted;e.currentTarget.style.borderColor=C.muted+"30";}}
                >
                  ↩ Restore
                </button>
              </Tooltip>
            </div>
          ))}
        </HCard>
      )}

      {/* Schema confirm cards */}
      <div style={{display:"flex",flexDirection:"column",gap:8,marginBottom:16}}>
        {Object.entries(mapping).map(([schema,fields])=>{
          const f=fields||[];
          const missN=f.filter((x,idx)=>(x.status==="missing")&&!ignored[ignoreKey(schema,idx)]).length;
          const reviewN=f.filter((x,idx)=>(x.status==="review")&&!ignored[ignoreKey(schema,idx)]).length;
          const autoN=f.filter(x=>["auto","system","manual"].includes(x.status)).length;
          const ignN=ignoredCount(schema);
          const ok=schemaOk(schema);
          const done=confirmed[schema];
          return (
            <HCard key={schema} style={{border:`1px solid ${done?C.green+"40":ok?C.border:C.yellow+"25"}`}}>
              <div style={{display:"flex",alignItems:"center",gap:12,flexWrap:"wrap"}}>
                <div style={{flex:1}}>
                  <div style={{display:"flex",alignItems:"center",gap:6,flexWrap:"wrap"}}>
                    <span style={{fontWeight:600,color:C.text,fontSize:13}}>{schema}</span>
                    {done&&<Pill color={C.green}>✓ Confirmed</Pill>}
                    {!ok&&(missN>0||reviewN>0)&&<Pill color={C.yellow}>⚠ Unresolved</Pill>}
                  </div>
                  <div style={{color:C.muted,fontSize:10,marginTop:2}}>
                    {autoN}/{f.length} fields mapped
                    {(missN>0||reviewN>0)&&!ok&&<span style={{color:C.yellow}}> · {missN+reviewN} unresolved</span>}
                    {ignN>0&&<span style={{color:C.muted}}> · {ignN} ignored</span>}
                  </div>
                </div>
                {!done&&(
                  <Btn sm color={ok?C.green:C.muted} disabled={!ok}
                    onClick={()=>setConfirmed(p=>({...p,[schema]:true}))}
                    tooltip={ok?"Confirm this schema":"Resolve or ignore all flagged fields first"}>
                    {ok?"Confirm ✓":"Resolve or ignore first"}
                  </Btn>
                )}
              </div>
            </HCard>
          );
        })}
      </div>

      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
        <div style={{display:"flex",gap:8}}>
          <Btn ghost onClick={onBack}>← Back</Btn>
          <Btn ghost color={C.cyan} onClick={()=>setExported(true)} tooltip="Download mapping as JSON">
            {exported?"✓ Exported":"⬇ Export Mapping"}
          </Btn>
        </div>
        <Btn onClick={handleNext} disabled={!allOk} loading={saving}
          tooltip={allOk?"Save and run validation":"Confirm all schemas first"}>
          {saving?"Saving...":allOk?"Run Validation →":"Confirm all schemas"}
        </Btn>
      </div>
    </div>
  );
}

// ── Step 5: Validation ────────────────────────────────────────────────────────
function ValidationStep({onNext,onBack,setValidationData}) {
  const [results,setResults]=useState(null);
  const [loading,setLoading]=useState(true);
  const [error,setError]=useState(null);
  const [open,setOpen]=useState(null);

  useEffect(()=>{
    api.validate()
      .then(res=>{ if(res.error) throw new Error(res.error); setResults(res); setValidationData(res); })
      .catch(e=>setError(e.message))
      .finally(()=>setLoading(false));
  },[]);

  if(loading) return <div style={{textAlign:"center",padding:60}}><Spinner/><div style={{color:C.muted,fontSize:12,marginTop:12}}>Running data quality checks...</div></div>;
  if(error) return <Alert type="error" msg={`Validation failed: ${error}`}/>;

  const schemas=Object.entries(results?.results||{});
  const passed=schemas.filter(([,v])=>v.passed).length;
  const totalW=schemas.reduce((a,[,v])=>a+(v.warnings?.length||0),0);
  const totalI=schemas.reduce((a,[,v])=>a+(v.issues?.length||0),0);

  return (
    <div>
      <div style={{marginBottom:20}}>
        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
          <div style={{fontSize:11,color:C.purple,letterSpacing:.8,textTransform:"uppercase",fontWeight:600}}>Step 6 of 8</div>
          <InfoBadge text="Automated quality checks — catches null values, type errors, duplicates and anomalies before ingestion."/>
        </div>
        <div style={{fontSize:20,fontWeight:700,color:C.text,marginBottom:4}}>Data Validation</div>
        <div style={{fontSize:12,color:C.textSub}}>Quality checks complete. Warnings are advisory.</div>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:8,marginBottom:16}}>
        <StatBox label="Schemas Passed" value={`${passed}/${schemas.length}`} color={C.green}  tooltip="Schemas passing all critical checks"/>
        <StatBox label="Critical Issues" value={totalI} color={totalI>0?C.red:C.green}          tooltip="Must be fixed before ingestion"/>
        <StatBox label="Warnings"        value={totalW} color={C.yellow}                         tooltip="Advisory — ingestion can proceed"/>
      </div>
      <div style={{display:"flex",flexDirection:"column",gap:6,marginBottom:14}}>
        {schemas.map(([schema,data],i)=>(
          <HCard key={schema} onClick={()=>setOpen(open===i?null:i)}
            style={{cursor:"pointer",border:`1px solid ${data.issues?.length>0?C.red+"30":data.warnings?.length>0?C.yellow+"25":C.green+"25"}`}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
              <div style={{display:"flex",alignItems:"center",gap:10}}>
                <div style={{width:8,height:8,borderRadius:"50%",background:data.passed?C.green:C.red,boxShadow:`0 0 6px ${data.passed?C.green:C.red}`,flexShrink:0}}/>
                <span style={{fontWeight:500,color:C.text,fontSize:12}}>{schema}</span>
              </div>
              <div style={{display:"flex",gap:6,alignItems:"center"}}>
                {data.issues?.length>0&&<Tag color={C.red}>{data.issues.length} errors</Tag>}
                {data.warnings?.length>0&&<Tag color={C.yellow}>{data.warnings.length} warnings</Tag>}
                {!data.issues?.length&&!data.warnings?.length&&<Tag color={C.green}>Clean</Tag>}
                <span style={{color:C.muted,fontSize:12}}>{open===i?"▲":"▼"}</span>
              </div>
            </div>
            {open===i&&(
              <div style={{marginTop:12,paddingTop:12,borderTop:`1px solid ${C.border}`}}>
                {[
                  ...(data.issues||[]).map(m=>({m,c:C.red,icon:"✕"})),
                  ...(data.warnings||[]).map(m=>({m,c:C.yellow,icon:"⚠"})),
                  ...(data.ai_insights||[]).map(ins=>({m:ins.issue||ins,c:C.cyan,icon:"ℹ"})),
                ].map(({m,c,icon},j)=>(
                  <div key={j} style={{display:"flex",gap:8,padding:"5px 0",borderBottom:`1px solid ${C.border}22`,fontSize:11,color:C.textSub}}>
                    <span style={{color:c,flexShrink:0}}>{icon}</span>
                    <span>{typeof m==="string"?m:m.issue||JSON.stringify(m)}</span>
                  </div>
                ))}
              </div>
            )}
          </HCard>
        ))}
      </div>
      {totalI===0&&(
        <div style={{background:`${C.green}08`,border:`1px solid ${C.green}20`,borderRadius:8,padding:"10px 14px",marginBottom:14,fontSize:11,color:C.textSub,display:"flex",gap:8}}>
          <span style={{color:C.green}}>✓</span>
          <span>All critical checks passed. {totalW} advisory warning{totalW!==1?"s":""} detected.</span>
        </div>
      )}
      <div style={{display:"flex",justifyContent:"space-between"}}>
        <Btn ghost onClick={onBack}>← Back</Btn>
        <Btn color={totalI>0?C.yellow:C.green} onClick={onNext}>
          {totalI>0?"Proceed with caution →":"Proceed to Ingestion →"}
        </Btn>
      </div>
    </div>
  );
}

// ── Step 6: Ingest ────────────────────────────────────────────────────────────
function IngestStep({onNext,onBack,setIngestResult}) {
  const [state,setState]=useState("idle");
  const [log,setLog]=useState([]);
  const [prog,setProg]=useState(0);
  const [error,setError]=useState(null);
  const wsRef=useRef(null);

  const tColor=t=>t==="AUTH"?C.muted:t==="ERD"?C.cyan:t==="FOLDER"?C.purple:t==="ENTRY"?C.purple:
    t==="DNA"?C.blue:t==="SAMPLE"?C.blueL:t==="INV"?C.yellow:t==="RESULT"?C.cyan:t==="FILE"?C.green:C.green;

  const run=()=>{
    setState("running"); setLog([]); setProg(0); setError(null);
    const ws=new WebSocket(`${WS}/ws/ingest`);
    wsRef.current=ws;
    ws.onmessage=e=>{
      const data=JSON.parse(e.data);
      if(data.status==="complete"){
        data.success?setState("done"):setState("error");
        setProg(100);
        if(!data.success) setError(data.error||"Ingestion failed");
        setIngestResult({success:data.success,error:data.error});
      } else if(data.msg){
        setLog(p=>[...p,{msg:data.msg,tag:data.tag||"INFO",ts:data.timestamp}]);
        if(data.status==="running") setProg(p=>Math.min(p+4,95));
      }
    };
    ws.onerror=()=>{ setState("error"); setError("WebSocket failed — is the backend running?"); };
  };
  useEffect(()=>()=>wsRef.current?.close(),[]);

  return (
    <div>
      <div style={{marginBottom:20}}>
        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
          <div style={{fontSize:11,color:C.purple,letterSpacing:.8,textTransform:"uppercase",fontWeight:600}}>Step 7 of 8</div>
          <InfoBadge text="Creates real records in Benchling. Do not close this tab while ingestion is running."/>
        </div>
        <div style={{fontSize:20,fontWeight:700,color:C.text,marginBottom:4}}>Ingest to Benchling</div>
        <div style={{fontSize:12,color:C.textSub}}>All validations passed. Records will be created in your selected notebook.</div>
      </div>
      {error&&<Alert type="error" msg={error}/>}
      {state==="idle"&&(
        <HCard style={{marginBottom:16}}>
          <div style={{background:`${C.yellow}08`,border:`1px solid ${C.yellow}20`,borderRadius:8,padding:"10px 14px",marginBottom:14,fontSize:11,color:C.textSub,display:"flex",gap:8}}>
            <span style={{color:C.yellow}}>⚠</span>
            <span>This will create live records in Benchling. Ensure you are using the correct dataset and destination notebook.</span>
          </div>
          <div style={{display:"flex",justifyContent:"flex-end"}}>
            <Btn color={C.green} onClick={run}>▶ Begin Ingestion</Btn>
          </div>
        </HCard>
      )}
      {(state==="running"||state==="done"||state==="error")&&(
        <HCard style={{marginBottom:14}}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:8}}>
            <span style={{fontSize:11,color:C.muted,fontWeight:500}}>
              {state==="running"?"Ingestion in progress — do not close this tab":state==="done"?"Ingestion complete":"Ingestion failed"}
            </span>
            <span style={{fontSize:12,fontFamily:"monospace",fontWeight:600,color:state==="done"?C.green:state==="error"?C.red:C.purple}}>{prog}%</span>
          </div>
          <div style={{background:"rgba(255,255,255,0.04)",borderRadius:6,height:6,marginBottom:14,overflow:"hidden"}}>
            <div style={{width:`${prog}%`,height:6,borderRadius:6,transition:"width .3s",
              background:state==="done"?`linear-gradient(90deg,${C.green},${C.greenL})`:
                state==="error"?C.red:`linear-gradient(90deg,${C.purple},${C.purpleL})`}}/>
          </div>
          <div style={{background:"#02020a",borderRadius:8,padding:14,maxHeight:240,overflowY:"auto",fontFamily:"monospace",fontSize:10,border:`1px solid ${C.border}`}}>
            {log.map((l,i)=>(
              <div key={i} style={{display:"flex",gap:10,marginBottom:4}}>
                <span style={{color:C.muted,flexShrink:0,fontSize:9}}>{l.ts}</span>
                <span style={{color:tColor(l.tag),minWidth:52,flexShrink:0,fontWeight:600}}>[{l.tag}]</span>
                <span style={{color:i===log.length-1&&state==="running"?C.text:C.textSub}}>{l.msg}</span>
              </div>
            ))}
            {state==="running"&&<div style={{color:C.purple}}>▌</div>}
          </div>
        </HCard>
      )}
      {state==="done"&&(
        <HCard style={{border:`1px solid ${C.green}30`,background:`${C.green}05`,marginBottom:14}}>
          <div style={{fontSize:10,color:C.green,letterSpacing:.5,textTransform:"uppercase",marginBottom:6,fontWeight:600}}>Ingestion Complete</div>
          <div style={{fontSize:12,color:C.textSub}}>All records created. View the report for full details and Benchling IDs.</div>
        </HCard>
      )}
      <div style={{display:"flex",justifyContent:"space-between"}}>
        <Btn ghost onClick={onBack} disabled={state==="running"}>← Back</Btn>
        <Btn onClick={onNext} disabled={state!=="done"}>View Report →</Btn>
      </div>
    </div>
  );
}

// ── Step 7: Report ────────────────────────────────────────────────────────────
function ReportStep({onRestart}) {
  const [report,setReport]=useState(null);
  const [loading,setLoading]=useState(true);
  const [tab,setTab]=useState("summary");

  useEffect(()=>{ api.report().then(setReport).catch(()=>{}).finally(()=>setLoading(false)); },[]);

  const parseReport=(content)=>{
    if(!content) return null;
    const records=[];
    const lines=content.split("\n");
    lines.forEach(line=>{
      if(!line.includes("✅")) return;
      // Match any Benchling ID: bfi_, seq_, con_, etr_, loc_, box_
      const idMatch=line.match(/(bfi_[\w]+|seq_[\w]+|con_[\w]+|etr_[\w]+|loc_[\w]+|box_[\w]+)/);
      if(!idMatch) return;
      let schema="Unknown";
      const ll=line.toLowerCase();
      if(ll.includes("dna")||ll.includes("seq_"))          schema="DNA Sequence";
      else if(ll.includes("sample")||ll.includes("bfi_"))  schema="Sample";
      else if(ll.includes("container")||ll.includes("con_")) schema="Container";
      else if(ll.includes("entry")||ll.includes("etr_"))   schema="Entry";
      else if(ll.includes("result"))                        schema="Results";
      else if(ll.includes("location")||ll.includes("loc_")) schema="Location";
      else if(ll.includes("box_"))                          schema="Box";
      const croMatch=line.match(/for CRO[:\s]+(\S+)/i)||line.match(/cro[:\s]+(\S+)/i);
      const nameMatch=line.match(/name=([^,\s]+)/);
      records.push({
        benchling_id: idMatch[1],
        schema,
        cro:  croMatch  ? croMatch[1].replace(",","")  : "—",
        name: nameMatch ? nameMatch[1] : "—",
        status:"success"
      });
    });
    const warnings=lines.filter(l=>l.includes("⚠")||l.toLowerCase().includes("warning"))
      .map(l=>l.replace(/.*⚠️?\s*/,"").trim()).filter(l=>l.length>5);
    const successMatch=content.match(/Successful\s*:\s*(\d+)/);
    const failedMatch=content.match(/Failed\s*:\s*(\d+)/);
    const dateMatch=content.match(/Run date\s*:\s*(.+)/);
    const dataMatch=content.match(/Data file\s*:\s*(.+)/);
    return {
      records, warnings,
      success:     content.includes("PIPELINE SUCCEEDED"),
      successful:  successMatch ? parseInt(successMatch[1]) : records.filter(r=>r.schema==="Sample").length,
      failed:      failedMatch  ? parseInt(failedMatch[1])  : 0,
      run_date:    dateMatch    ? dateMatch[1].trim()        : "—",
      data_file:   dataMatch    ? dataMatch[1].trim().split(/[\/\\]/).pop() : "—",
      raw: content
    };
  };

  const data=report?parseReport(report.content):null;

const downloadPDF=()=>{
  const now=new Date().toLocaleString();
    const recRows=(data?.records||[]).map(r=>`
              <tr>
                <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;font-family:monospace;font-size:11px;color:#6366f1">${r.benchling_id}</td>
                <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">${r.name!=="—"?r.name:r.benchling_id}</td>
                <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">${r.schema}</td>
                <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">${r.cro}</td>
                <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;color:#16a34a">✅ Success</td>
              </tr>`).join("").join("");
    const warnRows=(data?.warnings||[]).map(w=>`
      <div style="display:flex;gap:8px;padding:8px 0;border-bottom:1px solid #f3f4f6;font-size:11px">
        <span style="color:#d97706">⚠</span><span>${w}</span></div>`).join("");
    const html=`<!DOCTYPE html><html><head><meta charset="utf-8"/>
<title>Benchling Import Report</title>
<style>*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#fff;color:#1e1e2e;padding:32px;font-size:12px}
h1{font-size:22px;font-weight:700;margin-bottom:4px}
h2{font-size:13px;font-weight:600;color:#374151;margin:20px 0 10px;border-bottom:1px solid #e5e7eb;padding-bottom:6px}
.sub{color:#6b7280;font-size:11px;margin-bottom:24px}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.kpi{background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:14px;text-align:center}
.kpi .v{font-size:26px;font-weight:700;margin-bottom:2px}
.kpi .l{font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.4px}
.green{color:#059669}.blue{color:#2563eb}.yellow{color:#d97706}
table{width:100%;border-collapse:collapse;font-size:11px;margin-bottom:16px}
th{background:#f3f4f6;padding:7px 10px;text-align:left;font-weight:600;font-size:10px;text-transform:uppercase;letter-spacing:.4px;color:#374151}
td{padding:7px 10px;border-bottom:1px solid #f3f4f6}
.footer{margin-top:32px;padding-top:16px;border-top:1px solid #e5e7eb;color:#9ca3af;font-size:10px;text-align:center}
@media print{body{padding:16px}}</style></head><body>
<h1>Post-Ingestion Summary Report</h1>
<div class="sub">Benchling Data Importer · AI-Assisted · excelra.benchling.com · ${now}</div>
<div class="kpis">
  <div class="kpi"><div class="v blue">${data?.records?.length||0}</div><div class="l">Records Created</div></div>
  <div class="kpi"><div class="v green">100%</div><div class="l">Success Rate</div></div>
  <div class="kpi"><div class="v green">${data?.failed||0}</div><div class="l">Errors</div></div>
  <div class="kpi"><div class="v yellow">${data?.warnings?.length||0}</div><div class="l">Warnings</div></div>
</div>
<h2>Created Records</h2>
<table><thead><tr><th>Benchling ID</th><th>Schema</th><th>CRO</th><th>Status</th></tr></thead>
<tbody>${recRows||"<tr><td colspan='4' style='color:#9ca3af;text-align:center;padding:16px'>No records parsed</td></tr>"}</tbody></table>
<h2>Pipeline Log</h2>
<pre style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:6px;padding:14px;font-size:9px;white-space:pre-wrap;word-break:break-word;color:#374151">${report?.content||""}</pre>
<h2>Action Items</h2>${warnRows||"<p style='color:#6b7280;font-size:11px'>No warnings</p>"}
<div class="footer">Generated by Benchling Data Importer · ${now}</div>
<script>window.onload=()=>{window.print()}</script></body></html>`;
const blob=new Blob([html],{type:"text/html"});
  const url=URL.createObjectURL(blob);
  const a=document.createElement("a");
  a.href=url;
  a.download=`benchling_report_${new Date().toISOString().slice(0,10)}.html`;
  a.click();
  setTimeout(()=>URL.revokeObjectURL(url),2000);
};

  return (
    <div>
      <div style={{marginBottom:20}}>
        <div style={{fontSize:11,color:C.purple,letterSpacing:.8,textTransform:"uppercase",fontWeight:600,marginBottom:6}}>Step 8 of 8</div>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
          <div>
            <div style={{fontSize:20,fontWeight:700,color:C.text,marginBottom:4}}>Post-Ingestion Report</div>
            <div style={{fontSize:12,color:C.textSub}}>Full summary of this ingestion run.</div>
          </div>
          
        </div>
      </div>
      {loading&&<div style={{textAlign:"center",padding:40}}><Spinner/></div>}
      {!loading&&(
        <>
          <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:10,marginBottom:16}}>
            {[
              {l:"Records Created",v:data?.records?.length||0, c:C.green },
              {l:"Success Rate",   v:data?.failed===0?"100%":"Partial", c:C.green},
              {l:"Errors",        v:data?.failed||0,           c:data?.failed>0?C.red:C.green},
              {l:"Warnings",      v:data?.warnings?.length||0, c:data?.warnings?.length>0?C.yellow:C.green},
            ].map(k=>(
              <HCard key={k.l} style={{textAlign:"center",padding:"14px 10px"}}>
                <div style={{fontSize:26,fontWeight:700,color:k.c,lineHeight:1}}>{k.v}</div>
                <div style={{fontSize:10,color:C.muted,marginTop:4,letterSpacing:.5,textTransform:"uppercase"}}>{k.l}</div>
              </HCard>
            ))}
          </div>
          <HCard style={{marginBottom:12,padding:"10px 16px"}}>
            <div style={{display:"flex",gap:24,flexWrap:"wrap"}}>
              {[{l:"Run Date",v:data?.run_date||"—"},{l:"Duration",v:data?.duration||"—"},{l:"Report",v:report?.filename||"—"}].map(m=>(
                <div key={m.l}>
                  <div style={{fontSize:9,color:C.muted,textTransform:"uppercase",letterSpacing:.5,marginBottom:2}}>{m.l}</div>
                  <div style={{fontSize:11,color:C.text,fontWeight:500}}>{m.v}</div>
                </div>
              ))}
            </div>
          </HCard>
          <div style={{display:"flex",gap:4,marginBottom:12}}>
            {["summary","records","log"].map(t=>(
              <button key={t} onClick={()=>setTab(t)}
                style={{background:tab===t?C.purple:"transparent",color:tab===t?"#fff":C.muted,
                  border:`1px solid ${tab===t?C.purple:C.border}`,borderRadius:7,padding:"6px 14px",
                  cursor:"pointer",fontWeight:600,fontSize:11,transition:"all .15s",textTransform:"capitalize"}}>{t}</button>
            ))}
          </div>
          {tab==="summary"&&(
            <HCard style={{marginBottom:12}}>
              <div style={{fontSize:10,color:C.muted,letterSpacing:.5,textTransform:"uppercase",marginBottom:10,fontWeight:600}}>Action Items for Next Run</div>
              {data?.warnings?.length?data.warnings.map((w,i)=>(
                <div key={i} style={{display:"flex",gap:8,padding:"7px 0",borderBottom:i<data.warnings.length-1?`1px solid ${C.border}33`:"none",alignItems:"flex-start"}}>
                  <span style={{color:C.yellow,flexShrink:0}}>⚠</span>
                  <span style={{color:C.textSub,fontSize:11,lineHeight:1.5}}>{w}</span>
                </div>
              )):<div style={{color:C.green,fontSize:12}}>✓ No warnings detected</div>}
            </HCard>
          )}
          {tab==="records"&&(
            <HCard>
              <div style={{fontSize:10,color:C.muted,letterSpacing:.5,textTransform:"uppercase",marginBottom:10,fontWeight:600}}>Created Records</div>
              {data?.records?.length?(
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <thead>
                    <tr style={{color:C.muted,borderBottom:`1px solid ${C.border}`}}>
                      {["Benchling ID","Schema","CRO","Status"].map(h=>(
                        <th key={h} style={{padding:"5px 8px",textAlign:"left",fontWeight:500}}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.records.map((r,i)=>(
                      <tr key={i} style={{borderBottom:`1px solid ${C.border}33`}}
                        onMouseEnter={e=>e.currentTarget.style.background="rgba(255,255,255,0.02)"}
                        onMouseLeave={e=>e.currentTarget.style.background="transparent"}>
                        <td style={{padding:"7px 8px",fontFamily:"monospace",color:C.cyan,fontSize:10}}>{r.benchling_id}</td>
                        <td style={{padding:"7px 8px",color:C.muted}}>{r.schema}</td>
                        <td style={{padding:"7px 8px"}}><Tag color={C.blue}>{r.cro}</Tag></td>
                        <td style={{padding:"7px 8px"}}><Tag color={C.green}>✓ Created</Tag></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ):<div style={{color:C.muted,fontSize:12,padding:"20px 0",textAlign:"center"}}>No records found in log.</div>}
            </HCard>
          )}
          {tab==="log"&&(
            <HCard>
              <div style={{fontSize:10,color:C.muted,letterSpacing:.5,textTransform:"uppercase",marginBottom:10,fontWeight:600}}>Pipeline Log — {report?.filename}</div>
              <pre style={{background:"#02020a",borderRadius:8,padding:14,maxHeight:320,overflowY:"auto",
                fontFamily:"monospace",fontSize:10,color:C.textSub,border:`1px solid ${C.border}`,
                whiteSpace:"pre-wrap",wordBreak:"break-word",margin:0}}>{report?.content||"No log available"}</pre>
            </HCard>
          )}
        </>
      )}
<div style={{display:"flex",gap:8,justifyContent:"flex-end",marginTop:16}}>
  <Btn color={C.purple} onClick={downloadPDF}>⬇ Download PDF</Btn>
  <Btn onClick={onRestart}>New Run</Btn>
</div>
    </div>
  );
}

// ── App Shell ─────────────────────────────────────────────────────────────────
export default function App() {
  const [step,setStep]=useState(0);
  const [uploadData,setUploadData]=useState(null);
  const [erdData,setErdData]=useState(null);
  const [schemaSelections,setSchemaSelections]=useState(null);
  const [mappingData,setMappingData]=useState(null);
  const [approvedMapping,setApprovedMapping]=useState(null);
  const [validationData,setValidationData]=useState(null);
  const [ingestResult,setIngestResult]=useState(null);

  return (
    <div style={{background:C.bgGrad,minHeight:"100vh",
      fontFamily:"-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif",
      color:C.text,backgroundAttachment:"fixed"}}>
      <style>{`@keyframes spin{to{transform:rotate(360deg)}}`}</style>
      <div style={{position:"fixed",inset:0,pointerEvents:"none",zIndex:0,overflow:"hidden"}}>
        <div style={{position:"absolute",top:"-20%",left:"30%",width:600,height:600,
          background:"radial-gradient(circle,rgba(124,111,205,0.06) 0%,transparent 70%)",borderRadius:"50%"}}/>
        <div style={{position:"absolute",bottom:"-10%",right:"20%",width:500,height:500,
          background:"radial-gradient(circle,rgba(14,164,114,0.04) 0%,transparent 70%)",borderRadius:"50%"}}/>
      </div>
      <div style={{position:"relative",zIndex:1,padding:"20px 24px",maxWidth:920,margin:"0 auto"}}>
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",
          marginBottom:28,paddingBottom:16,borderBottom:`1px solid ${C.border}`}}>
          <div style={{display:"flex",alignItems:"center",gap:12}}>
            <div style={{width:36,height:36,borderRadius:8,background:`linear-gradient(135deg,${C.purple}33,${C.cyan}22)`,
              border:`1px solid ${C.purple}44`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:18}}>⬡</div>
            <div>
              <div style={{fontSize:14,fontWeight:700,color:C.text}}>Benchling Data Importer</div>
              <div style={{fontSize:10,color:C.muted,marginTop:1}}>AI-Assisted · ERD-Driven · Multi-Section · No mapping file required</div>
            </div>
          </div>
          <Tag color={C.cyan}>excelra.benchling.com</Tag>
        </div>
        <div style={{display:"flex",alignItems:"center",marginBottom:28}}>
          {STEPS.map((s,i)=>(
            <div key={i} style={{display:"flex",alignItems:"center",flex:i<STEPS.length-1?1:0}}>
              <div style={{display:"flex",flexDirection:"column",alignItems:"center"}}>
                <div style={{width:26,height:26,borderRadius:"50%",display:"flex",alignItems:"center",
                  justifyContent:"center",fontWeight:700,fontSize:10,
                  background:i<step?C.green:i===step?C.purple:"rgba(255,255,255,0.04)",
                  border:`1px solid ${i<step?C.green:i===step?C.purple:C.border}`,
                  color:i<=step?"#fff":C.muted,transition:"all .3s",
                  boxShadow:i===step?C.glow:i<step?C.glowG:"none"}}>
                  {i<step?"✓":i+1}
                </div>
                <div style={{fontSize:9,color:i===step?C.purple:i<step?C.green:C.muted,
                  marginTop:4,whiteSpace:"nowrap",letterSpacing:.3,fontWeight:i===step?600:400}}>{s}</div>
              </div>
              {i<STEPS.length-1&&(
                <div style={{flex:1,height:1,margin:"0 4px 14px",
                  background:i<step?`linear-gradient(90deg,${C.green},${C.green}88)`:C.border,transition:"background .3s"}}/>
              )}
            </div>
          ))}
        </div>
        <div style={{background:C.card,borderRadius:14,border:`1px solid ${C.border}`,padding:24,boxShadow:"0 20px 60px rgba(0,0,0,0.4)"}}>
          {step===0&&<NotebookStep        onNext={()=>setStep(1)}/>}
          {step===1&&<ImportStep          onNext={()=>setStep(2)} setUploadData={setUploadData} setErdData={setErdData}/>}
          {step===2&&<SchemaSelectionStep onNext={()=>setStep(3)} onBack={()=>setStep(1)} uploadData={uploadData} erdData={erdData} setSchemaSelections={setSchemaSelections}/>}
          {step===3&&<MappingStep         onNext={()=>setStep(4)} onBack={()=>setStep(2)} uploadData={uploadData} setMappingData={setMappingData}/>}
          {step===4&&<ReviewStep          onNext={()=>setStep(5)} onBack={()=>setStep(3)} mappingData={mappingData} uploadData={uploadData} setApprovedMapping={setApprovedMapping}/>}
          {step===5&&<ValidationStep      onNext={()=>setStep(6)} onBack={()=>setStep(4)} setValidationData={setValidationData}/>}
          {step===6&&<IngestStep          onNext={()=>setStep(7)} onBack={()=>setStep(5)} setIngestResult={setIngestResult}/>}
          {step===7&&<ReportStep          onRestart={()=>setStep(0)}/>}
        </div>
        <div style={{textAlign:"center",marginTop:16,fontSize:10,color:C.muted}}>
          Benchling Data Importer · AI-Assisted Pipeline · excelra.benchling.com
        </div>
      </div>
    </div>
  );
}