package org.apache.kyuubi.web.servlet;

import org.apache.kyuubi.common.KyuubiConfigs;
import org.apache.kyuubi.web.KyuubiWebServer;
import org.apache.kyuubi.web.conf.ViewModel;
import org.apache.kyuubi.web.dao.QueueDao;
import org.apache.kyuubi.web.model.KResult;
import org.apache.kyuubi.web.model.entity.KQueue;
import org.apache.kyuubi.web.model.view.KQueueView;
import org.apache.kyuubi.web.utils.KyuubiWebConf;
import org.apache.kyuubi.web.utils.ServletUtils;
import org.apache.kyuubi.web.utils.YarnUtils;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Path("queues")
public class QueueServlet {

    @GET
    @Produces(MediaType.TEXT_HTML)
    public ViewModel getQueues(@Context ServletContext context,
                               @Context HttpServletRequest req,
                               @Context HttpServletResponse res) throws IOException, YarnException {
        List<KQueue> queues = QueueDao.getInstance().getQueues();
        List<KQueueView> queueView = new ArrayList<>();
        KQueueView defaultQueue = new KQueueView("默认队列", KyuubiWebConf.load().get("spark.yarn.queue", "default"));

        YarnClient client = YarnUtils.createYarnClient();

        loadQueueStat(client, defaultQueue);
        queueView.add(defaultQueue);

        for (KQueue queue : queues) {
            KQueueView kQueueView = new KQueueView();
            kQueueView.setUsername(queue.getUsername());
            kQueueView.setQueue(queue.getQueue());
            loadQueueStat(client, kQueueView);
            queueView.add(kQueueView);
        }
        client.close();

        ViewModel viewModel = new ViewModel("/queues.html", req, res);
        viewModel.attr("queues", queueView);
        viewModel.attr("yarnUrl", YarnUtils.resolveBaseYarnUrl());
        return viewModel;

    }

    private void loadQueueStat(YarnClient client, KQueueView kQueueView) throws YarnException, IOException {
        QueueStatistics queueInfo = client.getQueueInfo(kQueueView.getQueue()).getQueueStatistics();
        String available = "Memory (MB): " + queueInfo.getAvailableMemoryMB() + ", VCores: " + queueInfo.getAvailableVCores();
        kQueueView.setAvailable(available);

        String used = "Memory (MB): " + queueInfo.getAllocatedMemoryMB() + ", VCores: " + queueInfo.getAllocatedVCores();
        kQueueView.setUsed(used);
    }

    @POST
    @Path("addOrUpdate")
    @Produces(MediaType.APPLICATION_JSON)
    public KResult addOrUpdate(@Context HttpServletRequest req,
                               @FormParam("username") String username,
                               @FormParam("queue") String queue) throws IOException, YarnException {
        boolean isAdmin = ServletUtils.isAdmin(req);

        if (!isAdmin) {
            return KResult.failure("You do not have permission to perform this operation");
        }
        if (queue == null || queue.isEmpty()) {
            return KResult.failure("队列名不能为空");
        }
        if (username == null || username.isEmpty()) {
            return KResult.failure("用户名不能为空");
        }
        YarnClient client = YarnUtils.createYarnClient();
        QueueInfo queueInfo = client.getQueueInfo(queue);

        if (queueInfo == null) {
            return KResult.failure("队列不存在");
        }
        if (username.equals("default")) {
            return KResult.failure("不支持修改用户的默认队列");
        }
        client.close();
        QueueDao.getInstance().saveOrUpdate(new KQueue(username, queue));
        return KResult.success("add success");
    }

    @POST
    @Path("del")
    @Produces(MediaType.APPLICATION_JSON)
    public KResult delete(@Context HttpServletRequest req,
                          @FormParam("username") String username) {
        String loginUser = (String) req.getSession().getAttribute(KyuubiWebServer.LOGIN_USER_ATTR);
        String[] adminUsers = KyuubiWebConf.load().get(KyuubiConfigs.KYUUBI_AUDIT_ADMIN).split(",");
        boolean isAdmin = Arrays.stream(adminUsers).anyMatch(it -> it.equals(loginUser));

        if (!isAdmin) {
            return KResult.failure("You do not have permission to perform this operation");
        }
        QueueDao.getInstance().softDeleteByUser(username);

        return KResult.success("delete success");
    }
}
