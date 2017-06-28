@Grab('com.fasterxml.jackson.core:jackson-annotations:2.8.8')
@Grab('com.fasterxml.jackson.core:jackson-core:2.8.8')
@Grab('com.fasterxml.jackson.core:jackson-databind:2.8.8')

import com.github.pgasync.*;
import com.github.pgasync.impl.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.*;
import java.nio.charset.Charset;
import groovy.sql.*;

String DRIVER = "org.postgresql.Driver";
String URL = "jdbc:postgresql://tscale:5432/tscale";
String USER = "tscale";
String PASS = "tscale";

bigDecArrToLongArr = { arr ->
    int len = arr.length;
    long[] larr = new long[len];
    for(i in 0..len-1) {
        larr[i] = arr[i].longValue();
    }
    return larr;
}

// select regexp_split_to_array(regexp_split_to_table(replace('dc=dcx,host=mad-server,app   =os-agent,cpu=0,type=combined,*', ' ', ''), ','), '=')

METRSTR = "dc=dcx,host=mad-server,app   =os-agent,cpu=0,type=*,*";

pg = Sql.newInstance(URL, USER, PASS, DRIVER);
boolean grouped = false;
tagPairIdSets = [];
pg.eachRow("select regexp_split_to_array(regexp_split_to_table(replace($METRSTR, ' ', ''), ','), '=') TP", {
    String[] kv = it.TP.getArray();
    if(kv.length==1) {
        if("*".equals(kv[0])) {
            grouped = true;   
        } else {
            throw new RuntimeException("Invalid tag $kv in expression [$METRSTR]");
        }
    } else {
        kpat = kv[0].replace('*', '%');
        vpat = kv[1].replace('*', '%');
        pg.eachRow("select array_agg(distinct tagpair_id) TPS from tsd_tagpair t, tsd_tagk k, tsd_tagv v where t.tagk_id = k.tagk_id and t.tagv_id = v.tagv_id and k.name like ? and v.name like ?", [kpat,vpat], {
            //println "ARR: ${it.TPS.getArray()} -- ${it.TPS.getArray().length}";
            larr = bigDecArrToLongArr(it.TPS.getArray());
            println "$kv --> [$larr]";
            
        });
    }
    
});

