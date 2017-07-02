@Grab('com.github.alaisi.pgasync:postgres-async-driver:0.9')
@Grab('com.fasterxml.jackson.core:jackson-annotations:2.8.8')
@Grab('com.fasterxml.jackson.core:jackson-core:2.8.8')
@Grab('com.fasterxml.jackson.core:jackson-databind:2.8.8')
@Grab('io.dropwizard.metrics:metrics-core:3.1.2')


import org.helios.nativex.sigar.HeliosSigar;
import org.hyperic.sigar.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import org.hyperic.sigar.ptql.*;
import java.lang.management.*;
import java.nio.charset.Charset;
import com.github.pgasync.*;
import com.github.pgasync.impl.*;
import com.github.pgasync.impl.conversion.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.node.*;
import java.nio.charset.Charset;
import com.codahale.metrics.*;

class JSONConverter implements Converter<JsonNode> {
    final ObjectMapper jsonMapper = new ObjectMapper();
    final Charset UTF8 = Charset.forName("UTF8");
    public Class<JsonNode> type() {
        return JsonNode.class;
    }

    public byte[] from(JsonNode o) {
        return jsonMapper.writeValueAsBytes(o);
    }

    public JsonNode to(Oid oid, byte[] value) {
        switch(oid) {
            case Oid.JSON:
            case Oid.JSONB:
                return jsonMapper.readValue(value, 0, value.length, JsonNode.class);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> JsonNode");        
        }
        
    }    
}

class JSONArrayConverter implements Converter<ArrayNode> {
    final ObjectMapper jsonMapper = new ObjectMapper();
    final Charset UTF8 = Charset.forName("UTF8");
    public Class<JsonNode> type() {
        return ArrayNode.class;
    }

    public byte[] from(ArrayNode o) {
        return jsonMapper.writeValueAsBytes(o);
    }

    public ArrayNode to(Oid oid, byte[] value) {
        switch(oid) {
            case Oid.JSON:
            case Oid.JSONB:
                return jsonMapper.readValue(value, 0, value.length, ArrayNode.class);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> ArrayNode");        
        }
        
    }    
}

class NumberConverter implements Converter<Number[]> {
    final Charset UTF8 = Charset.forName("UTF8");

    public Class<Number[]> type() {
        return Number[].class;
    }

    public byte[] from(Number[] o) {
        return ArrayConversions.fromArray(o, {n -> return "$n".getBytes(UTF8)});
    }

    public Number[] to(Oid oid, byte[] value) {
        return ArrayConversions.toArray(Number.class, oid, value, {o, bytes -> 
            String s = new String(bytes, UTF8);
            if(s.contains(".")) return new Double(s);
            return Long.parseLong(s);
        });
    }    
}

// BiFunction<Oid,byte[],Object> parse



Db db = new ConnectionPoolBuilder()
        .hostname("tscale")
        .port(5432)
        .database("tscale")
        .username("tscale")
        .password("tscale")
        .poolSize(20)
        .converters(new JSONConverter(), new JSONArrayConverter(), new NumberConverter())
        .build();

db.listen("ERRORS").subscribe({msg -> println "\tNOTIFY ---------> $msg"});

ObjectMapper objectMapper = new ObjectMapper();
JsonNodeFactory nodeFactory = new JsonNodeFactory(false);

final String INS_SQL = "INSERT INTO TSDB (time, fqnid, value) VALUES "
def sigar = HeliosSigar.getInstance();
def UTF8 = Charset.forName("UTF8");
def buff = nodeFactory.arrayNode();
def fastSqlBuff = new StringBuilder(INS_SQL);
def fastBuffered = new AtomicInteger(0);
def metricSet = new HashSet<String>(1024);
def processFinder = new ProcessFinder(sigar.getSigar());
def idMap = new ConcurrentHashMap<String, Long>(2048, 0.75, 8);
def times = [];
def ids = [];
def vals = [];
def registry = new MetricRegistry();
def textPutTimer = registry.timer("textPut");
def fastPutTimer = registry.timer("fastPut");

def textPutHist = registry.histogram("textPutPer");
def fastPutHist = registry.histogram("fastPutPer");




//ConsoleReporter.forRegistry(registry).outputTo(System.out).build().start(5, TimeUnit.SECONDS);
JmxReporter.forRegistry(registry)    
    .build()
    .start();


HOST = "crazy-server";
DC = "jfk";
stime = {
    return System.currentTimeMillis();
}

toMetricName = { metric, tags ->
    def b = new StringBuilder(metric).append(":");
    if(!tags.containsKey("dc")) tags.put("dc", DC);
    if(!tags.containsKey("host")) tags.put("host", HOST);
    if(!tags.containsKey("app")) tags.put("app", "os-agent");
    sorted = new TreeMap<String, String>(tags);
    sorted.each() { k,v ->
        b.append("${k.trim().toLowerCase()}=${v.toString().trim().toLowerCase().replace(',', '_')},")
    }
    return b.deleteCharAt(b.length()-1).toString();
}

sortMetricName = { name ->
    try {
        String[] frags = name.split(":");
        def tags = [:];
        frags[1].split(",").each() { tp ->
            if(tp.contains("=")) {
                String[] kv = tp.split("=");
                tags.put(kv[0], kv[1]);
            }
        }
        return toMetricName(frags[0], tags);
    } catch (x) {
        println "Bad parse on name [$name]";
        throw x;
    }
}

numListToArr = { list -> 
    int size = list.size();
    Number[] arr = new Number[size];
    for (i in 0..size-1) {
        arr[i] = list.get(i);
    }
    return arr;
}


flush = { name ->
    if(buff.size() > 0) {
        println "Metrics [$name]: ${buff.size()}";
        // String lastEntry = objectMapper.writeValueAsString(buff.get(buff.size()-1));
        // String lastEntry2 = objectMapper.writeValueAsString(buff.get(buff.size()-2));
        // println "Last: $lastEntry";
        // println "Last2: $lastEntry2";
        String jsonPayload = objectMapper.writeValueAsString(buff);
        //println jsonPayload;
        final int count = buff.size();
        def ctx = textPutTimer.time();
        db.queryRows("select tsd_id_for_ids(jsonb('$jsonPayload'))")        
        .map({row -> row.get(0, JsonNode.class) })  
        .toBlocking()      
        .subscribe({ t ->   // ObjectNode
            //println "ID Responses: ${t.size()}"
            long elapsed = ctx.stop();
            textPutHist.update((elapsed/count/1000).longValue());
            trace("text.per.elapsed", (elapsed/count/1000).longValue(), ["unit": "ms"]);
            t.fieldNames().toList().each() { metricName -> 
                long id = t.findValue(metricName).asLong();
                String sortedMetricName = sortMetricName(metricName);
                if(idMap.putIfAbsent(sortedMetricName, id)==null) {
                    if(sortedMetricName.startsWith("sys.mem:")) {
                        //println "Cached ID $id for $sortedMetricName";
                    }
                }
                
            }
        },
        {exc -> 
            println "------->  tsd_id_for_ids ERROR";
            exc.printStackTrace(System.err);
            println "========================================================================================";
            println jsonPayload;
            println "========================================================================================";
        });
        //latch.await();
        buff.removeAll();
        metricSet.clear();        
        
    }
    if(fastBuffered.get() > 0) {
        final int count = fastBuffered.get();
        def ctx = fastPutTimer.time();
        db.queryRows('select fast_put($1, $2, $3)', numListToArr(times), numListToArr(ids), numListToArr(vals))
        .map({row -> row.get(0, Number[].class) })
        .subscribe({result -> 
            //println("Fast Inserts: ${result}");
            long elapsed = ctx.stop();
            fastPutHist.update((elapsed/count/1000).longValue());
            trace("fast.per.elapsed", (elapsed/count/1000).longValue(), ["unit": "ms"]);
            
        },{ exc ->
            println "------->  fastInsert ERROR: $exc";

            });
        fastBuffered.set(0);
        times.clear();
        ids.clear();
        vals.clear();
    }
}

pflush = { 
    println objectMapper.writeValueAsString(buff);
    buff.removeAll();
}



trace = { metric, value, tags ->
    if(!tags.isEmpty()) {
        now = System.currentTimeMillis();
        String key = toMetricName(metric, tags);
        Long tid = idMap.get(key);
        if(tid==null) {
            def met = nodeFactory.objectNode();
            met.put("timestamp", now);
            met.put("metric", metric);
            met.put("value", value);
            def tgs = nodeFactory.objectNode();
            if(!tags.containsKey("dc")) tags.put("dc", DC);
            if(!tags.containsKey("host")) tags.put("host", HOST);
            if(!tags.containsKey("app")) tags.put("app", "os-agent");
            tags.each() { k, v ->
                tgs.put("${k.trim().toLowerCase()}", v);
            }

            met.put("tags", tgs);
            if(metricSet.add(objectMapper.writeValueAsString(met))) {
                buff.add(met);
            } 
        } else {
            fastBuffered.incrementAndGet();
            times.add(now);
            ids.add(tid);
            vals.add(value);
            //fastSqlBuff.append("(to_timestamp($now::double precision / 1000), $tid, $value),")
        }
    }
}

// {
//     "metric": "sys.cpu.nice",
//     "timestamp": 1346846400,
//     "value": 18,
//     "tags": {
//        "host": "web01",
//        "dc": "lga"
//     }
// }


ctrace = { metric, value, tags ->
    if(value!=-1) {
        trace(metric, value, tags);
    }
}


try {
    long loop = 0;
    while(true) {
        long start = System.currentTimeMillis();
        loop++;        
        sigar.getCpuPercList().eachWithIndex() { cpu, index ->
            trace("sys.cpu", cpu.getCombined()*100, ['cpu':index, 'type':'combined']);
            trace("sys.cpu", cpu.getIdle()*100, ['cpu':index, 'type':'idle']);
            trace("sys.cpu", cpu.getIrq()*100, ['cpu':index, 'type':'irq']);
            trace("sys.cpu", cpu.getNice()*100, ['cpu':index, 'type':'nice']);
            trace("sys.cpu", cpu.getSoftIrq()*100, ['cpu':index, 'type':'softirq']);
            trace("sys.cpu", cpu.getStolen()*100, ['cpu':index, 'type':'stolen']);
            trace("sys.cpu", cpu.getSys()*100, ['cpu':index, 'type':'sys']);
            trace("sys.cpu", cpu.getUser()*100, ['cpu':index, 'type':'user']);
            trace("sys.cpu", cpu.getWait()*100, ['cpu':index, 'type':'wait']);                
        }
        flush("CPU"); 
        sigar.getFileSystemList().each() { fs ->
            //println "FS: dir:${fs.getDirName()},  dev:${fs.getDevName()}, type:${fs.getSysTypeName()}, opts:${fs.getOptions()}";
            try {
                fsu = sigar.getFileSystemUsage(fs.getDirName());
                ctrace("sys.fs.avail", fsu.getAvail(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
                ctrace("sys.fs.queue", fsu.getDiskQueue(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
                ctrace("sys.fs.files", fsu.getFiles(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
                ctrace("sys.fs.free", fsu.getFree(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
                ctrace("sys.fs.freefiles", fsu.getFreeFiles(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
                ctrace("sys.fs.total", fsu.getTotal(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
                ctrace("sys.fs.used", fsu.getUsed(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
                ctrace("sys.fs.usedperc", fsu.getUsePercent(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
                
                ctrace("sys.fs.bytes", fsu.getDiskReadBytes(), ['name':fs.getDirName(), 'type':fs.getSysTypeName(), 'dir':'reads']);
                ctrace("sys.fs.bytes", fsu.getDiskWriteBytes(), ['name':fs.getDirName(), 'type':fs.getSysTypeName(), 'dir':'writes']);

                ctrace("sys.fs.ios", fsu.getDiskReads(), ['name':fs.getDirName(), 'type':fs.getSysTypeName(), 'dir':'reads']);
                ctrace("sys.fs.ios", fsu.getDiskWrites(), ['name':fs.getDirName(), 'type':fs.getSysTypeName(), 'dir':'writes']);
            } catch (x) {}

            
            //flush();
            //println "[$fs]: $fsu";
        }
        flush("FS"); 

        sigar.getNetInterfaceList().each() { iface ->
            ifs = sigar.getNetInterfaceStat(iface);
            trace("sys.net.iface", ifs.getRxBytes(), ['name':iface, 'dir':'rx', 'unit':'bytes']);
            trace("sys.net.iface", ifs.getRxPackets(), ['name':iface, 'dir':'rx', 'unit':'packets']);
            trace("sys.net.iface", ifs.getRxDropped(), ['name':iface, 'dir':'rx', 'unit':'dropped']);
            trace("sys.net.iface", ifs.getRxErrors(), ['name':iface, 'dir':'rx', 'unit':'errors']);
            trace("sys.net.iface", ifs.getRxOverruns(), ['name':iface, 'dir':'rx', 'unit':'overruns']);
            trace("sys.net.iface", ifs.getRxFrame(), ['name':iface, 'dir':'rx', 'unit':'frame']);
            
            trace("sys.net.iface", ifs.getTxBytes(), ['name':iface, 'dir':'tx', 'unit':'bytes']);
            trace("sys.net.iface", ifs.getTxPackets(), ['name':iface, 'dir':'tx', 'unit':'packets']);
            trace("sys.net.iface", ifs.getTxDropped(), ['name':iface, 'dir':'tx', 'unit':'dropped']);
            trace("sys.net.iface", ifs.getTxErrors(), ['name':iface, 'dir':'tx', 'unit':'errors']);
            trace("sys.net.iface", ifs.getTxOverruns(), ['name':iface, 'dir':'tx', 'unit':'overruns']);

            //println ifs;
            //flush();
        }
        flush("IFACE"); 
        
        tcp = sigar.getTcp();
        trace("sys.net.tcp", tcp.getRetransSegs(), ['type':'RetransSegs']);
        trace("sys.net.tcp", tcp.getPassiveOpens(), ['type':'PassiveOpens']);
        trace("sys.net.tcp", tcp.getCurrEstab(), ['type':'CurrEstab']);
        trace("sys.net.tcp", tcp.getEstabResets(), ['type':'EstabResets']);
        trace("sys.net.tcp", tcp.getAttemptFails(), ['type':'AttemptFails']);
        trace("sys.net.tcp", tcp.getInSegs(), ['type':'InSegs']);
        trace("sys.net.tcp", tcp.getActiveOpens(), ['type':'ActiveOpens']);
        trace("sys.net.tcp", tcp.getInErrs(), ['type':'InErrs']);        
        trace("sys.net.tcp", tcp.getOutRsts(), ['type':'OutRsts']);        
        trace("sys.net.tcp", tcp.getOutSegs(), ['type':'OutSegs']);       
        
        flush("tcp"); 


        netstat = sigar.getNetStat();
        
        //===================================================================================================================
        //        INBOUND
        //===================================================================================================================
        trace("sys.net.socket", netstat.getAllInboundTotal(), ['dir':'inbound', 'protocol':'all', 'state':'all']);
        trace("sys.net.socket", netstat.getTcpInboundTotal(), ['dir':'inbound', 'protocol':'tcp', 'state':'all']);       
        trace("sys.net.socket", netstat.getTcpBound(), ['dir':'inbound', 'protocol':'tcp', 'state':'bound']);
        trace("sys.net.socket", netstat.getTcpListen(), ['dir':'inbound', 'protocol':'tcp', 'state':'lastack']);        
        trace("sys.net.socket", netstat.getTcpLastAck(), ['dir':'inbound', 'protocol':'tcp', 'state':'lastack']);        
        trace("sys.net.socket", netstat.getTcpCloseWait(), ['dir':'inbound', 'protocol':'tcp', 'state':'closewait']);
        flush("INBOUND"); 

        
        //===================================================================================================================
        //        OUTBOUND
        //===================================================================================================================
        trace("sys.net.socket", netstat.getAllOutboundTotal(), ['dir':'outbound', 'protocol':'all', 'state':'all']);
        trace("sys.net.socket", netstat.getTcpOutboundTotal(), ['dir':'outbound', 'protocol':'tcp', 'state':'all']);        
        trace("sys.net.socket", netstat.getTcpSynRecv(), ['dir':'outbound', 'protocol':'tcp', 'state':'synrecv']);        
        trace("sys.net.socket", netstat.getTcpSynSent(), ['dir':'outbound', 'protocol':'tcp', 'state':'synsent']);        
        trace("sys.net.socket", netstat.getTcpEstablished(), ['dir':'outbound', 'protocol':'tcp', 'state':'established']);
        trace("sys.net.socket", netstat.getTcpClose(), ['dir':'outbound', 'protocol':'tcp', 'state':'close']);
        trace("sys.net.socket", netstat.getTcpClosing(), ['dir':'outbound', 'protocol':'tcp', 'state':'closing']);
        trace("sys.net.socket", netstat.getTcpFinWait1(), ['dir':'outbound', 'protocol':'tcp', 'state':'finwait1']);
        trace("sys.net.socket", netstat.getTcpFinWait2(), ['dir':'outbound', 'protocol':'tcp', 'state':'finwait2']);
        trace("sys.net.socket", netstat.getTcpIdle(), ['dir':'outbound', 'protocol':'tcp', 'state':'idle']);
        trace("sys.net.socket", netstat.getTcpTimeWait(), ['dir':'outbound', 'protocol':'tcp', 'state':'timewait']);        
        flush("OUTBOUND"); 

        //===================================================================================================================
        //        SERVER SOCKETS
        //===================================================================================================================        
        connMap = new TreeMap<String, TreeMap<String, TreeMap<String, AtomicInteger>>>();
        sigar.getNetConnectionList(NetFlags.CONN_SERVER | NetFlags.CONN_PROTOCOLS).each() {
            addr = InetAddress.getByName(it.getLocalAddress()).getHostAddress();
            port = "${addr}:${it.getLocalPort()}";
            state = it.getStateString();
            protocol = it.getTypeString();
            stateMap = connMap.get(port);
            if(stateMap==null) {
                stateMap = new TreeMap<String, TreeMap<String, Integer>>();
                connMap.put(port, stateMap);
            }
            protocolMap = stateMap.get(state);
            if(protocolMap==null) {
                protocolMap = new TreeMap<String, AtomicInteger>();
                stateMap.put(state, protocolMap);
            }
            counter = protocolMap.get(protocol);
            if(counter==null) {
                counter = new AtomicInteger(0);
                protocolMap.put(protocol, counter);
            }
            counter.incrementAndGet();            
        }
        connMap.each() { port, stateMap ->
            stateMap.each() { state, protocolMap ->
                protocolMap.each() { protocol, counter ->
                    index = port.lastIndexOf(":");
                    addr = port.substring(0, index);
                    p = port.substring(index+1);
                    //println "Port: $port, State: $state, Protocol: $protocol, Count: ${counter.get()}";
                    trace("sys.net.server", counter.get(), ['protocol':protocol, 'state':state.toLowerCase(), 'port':p, 'bind':addr]);
                }
            }
        }
        flush("SERVER SOCKS"); 

        //===================================================================================================================
        //        CLIENT SOCKETS
        //===================================================================================================================        
        connMap = new TreeMap<String, TreeMap<String, TreeMap<String, AtomicInteger>>>();
        sigar.getNetConnectionList(NetFlags.CONN_CLIENT | NetFlags.CONN_PROTOCOLS).each() {
            addr = InetAddress.getByName(it.getRemoteAddress()).getHostAddress();
            port = "${addr}:${it.getRemotePort()}";
            state = it.getStateString();
            protocol = it.getTypeString();
            stateMap = connMap.get(port);
            if(stateMap==null) {
                stateMap = new TreeMap<String, TreeMap<String, Integer>>();
                connMap.put(port, stateMap);
            }
            protocolMap = stateMap.get(state);
            if(protocolMap==null) {
                protocolMap = new TreeMap<String, AtomicInteger>();
                stateMap.put(state, protocolMap);
            }
            counter = protocolMap.get(protocol);
            if(counter==null) {
                counter = new AtomicInteger(0);
                protocolMap.put(protocol, counter);
            }
            counter.incrementAndGet();            
        }
        connMap.each() { port, stateMap ->
            stateMap.each() { state, protocolMap ->
                protocolMap.each() { protocol, counter ->
                    index = port.lastIndexOf(":");
                    addr = port.substring(0, index);
                    p = port.substring(index+1);
                    //println "Port: $port, State: $state, Protocol: $protocol, Count: ${counter.get()}";
                    trace("sys.net.client", counter.get(), ['protocol':protocol, 'state':state.toLowerCase(), 'port':p, 'address':addr]);
                }
            }
        }    
        flush("CLIENT SOCKS"); 

        //flush();
        // ===================================================================================================================================
        //        SYSTEM MEMORY
        // ===================================================================================================================================
        mem = sigar.getMem();
        
        trace("sys.mem", mem.getUsed(), ['unit':'used']);       
        trace("sys.mem", mem.getFree(), ['unit':'used']);       
        
        trace("sys.mem.actual", mem.getActualFree(), ['unit':'free']);       
        trace("sys.mem.actual", mem.getActualUsed(), ['unit':'used']);       
        
        trace("sys.mem.total", mem.getTotal(), ['unit':'bytes']);       
        trace("sys.mem.total", mem.getRam(), ['unit':'MB']);       
        
        trace("sys.mem.percent", mem.getFreePercent(), ['unit':'free']);       
        trace("sys.mem.percent", mem.getUsedPercent(), ['unit':'used']);       
        
        flush("SYSMEM"); 

        // ===================================================================================================================================
        //    SWAP
        // ===================================================================================================================================
        swap = sigar.getSwap();
        swapFree = swap.getFree();
        swapUsed = swap.getUsed();
        swapTotal = swap.getTotal();
        trace("sys.swap", swapFree, ['unit': 'free']);
        trace("sys.swap", swapUsed, ['unit': 'used']);
        trace("sys.swap", swapTotal, ['unit': 'total']);
        trace("sys.swap.percent", swapUsed/swapTotal*100, ['unit': 'used']);
        trace("sys.swap.percent", swapFree/swapTotal*100, ['unit': 'free']);
        trace("sys.swap.page", swap.getPageIn(), ['dir': 'in']);
        trace("sys.swap.page", swap.getPageOut(), ['dir': 'out']);
        flush("SWAP"); 

        //flush();
        // ===================================================================================================================================
        //    PROCESS STATS
        // ===================================================================================================================================
        procStat = sigar.getProcStat();
        trace("sys.procs.state", procStat.getIdle(), ['state': 'idle']);
        trace("sys.procs.state", procStat.getRunning(), ['state': 'running']);
        trace("sys.procs.state", procStat.getSleeping(), ['state': 'sleeping']);
        trace("sys.procs.state", procStat.getStopped(), ['state': 'stopped']);
        trace("sys.procs.state", procStat.getZombie(), ['state': 'zombie']);
        
        trace("sys.procs.threads", procStat.getThreads(), [:]);
        trace("sys.procs.count", procStat.getTotal(), [:]);
        flush("PROCSTATS"); 

        //flush();
        // ===================================================================================================================================
        //    LOAD AVERAGE
        // ===================================================================================================================================
        double[] load = sigar.getLoadAverage();
        trace("sys.load", load[0], ['period': '1m']);
        trace("sys.load", load[1], ['period': '5m']);
        trace("sys.load", load[2], ['period': '15m']);
        flush("LOAD"); 

        //flush();
        
        // ===================================================================================================================================
        //    PROCESS GROUPS
        // ===================================================================================================================================
        processQueries = [
            "sshd" : "State.Name.ew=sshd",
            "apache2": "State.Name.ew=apache2",
            "java": "State.Name.ew=java",
            "python": "State.Name.ct=python"
        ];     
        // ===================================================================================================================================
        //    PROCESS GROUP CPU STATS
        // ===================================================================================================================================
        processQueries.each() { exe, query ->
            mcpu = sigar.getMultiProcCpu(query);
            trace("procs", mcpu.getPercent() * 100, ['exe':exe, 'unit':'percentcpu']);
            trace("procs", mcpu.getProcesses(), ['exe':exe, 'unit':'count']);            
        }
        flush("PROC CPU"); 

        //flush();
        // ===================================================================================================================================
        //    PROCESS GROUP MEM STATS
        // ===================================================================================================================================
        processQueries.each() { exe, query ->
            
            mmem = sigar.getMultiProcMem(query);

            trace("procs", mmem.getMajorFaults(), ['exe':exe, 'unit':'majorfaults']);
            trace("procs", mmem.getMinorFaults(), ['exe':exe, 'unit':'minorfaults']);            
            trace("procs", mmem.getPageFaults(), ['exe':exe, 'unit':'pagefaults']);            
            trace("procs", mmem.getResident(), ['exe':exe, 'unit':'resident']);            
            trace("procs", mmem.getShare(), ['exe':exe, 'unit':'share']);            
            trace("procs", mmem.getSize(), ['exe':exe, 'unit':'size']);            
            trace("procs", mmem.getSize(), ['exe':exe, 'unit':'size']);            
        }
        flush("PROC MEM"); 
        
        
        
        
        //println tcp;

        //NetFlags.CONN_TCP | NetFlags.CONN_CLIENT
        sigar.getNetConnectionList(NetFlags.CONN_SERVER | NetFlags.CONN_UDP ).each() {
            //println "SendQueue=${it.getSendQueue()}, ReceiveQueue=${it.getReceiveQueue()}, State=${it.getStateString()}, Type=${it.getTypeString()}, LocalPort=${it.getLocalPort()}, RemoteAddress=${it.getRemoteAddress()}, RemotePort=${it.getRemotePort()}";
        }
        
        long elapsed = System.currentTimeMillis() - start;
        println "IDMap: ${idMap.size()}";
        println "====================="
        if(loop%10==0) println "Scan #${loop} complete in $elapsed ms.";
        //break;
        Thread.sleep(2000);
    }
    
    
        //trace(tsdbSocket);
        //     trace(tsdbSocket, "put sys.cpu.user ${stime()} 42.5 host=webserver1 cpu=0\n");
        //println "${it.getCombined()*100}  -  ${it.format(it.getCombined())}";
    //}

} finally {
    try { db.close(); } catch (e) {}
    println "Closed";
}

return null;