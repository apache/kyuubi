package org.apache.kyuubi.web.servlet;

import org.apache.kyuubi.common.KyuubiConfigs;
import org.apache.kyuubi.web.conf.ViewModel;
import org.apache.kyuubi.web.utils.KyuubiWebConf;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Path("envs")
public class EnvServlet {

    @GET
    @Produces(MediaType.TEXT_HTML)
    public ViewModel getEnvs(@Context ServletContext context,
                             @Context HttpServletRequest req,
                             @Context HttpServletResponse res) {
        KyuubiWebConf conf = KyuubiWebConf.load();
        ViewModel viewModel = new ViewModel("/envs.html", req, res);
        Map<String, String> envs = new TreeMap<>(conf.getAll());
        List<String> secretConfNames = Arrays.stream(KyuubiConfigs.values())
                .map(KyuubiConfigs::getKey)
                .filter(it -> it.contains("passwd") || it.contains("pwd"))
                .collect(Collectors.toList());
        for (String secretConfName : secretConfNames) {
            envs.remove(secretConfName);
        }

        viewModel.attr("envs", envs);
        return viewModel;
    }
}
