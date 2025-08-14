/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.core.distributed.distro.task.verify;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.task.execute.DistroExecuteTaskExecuteEngine;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.List;

/**
 * Timed to start distro verify task.
 *
 * @author xiweng.yy
 */
public class DistroVerifyTimedTask implements Runnable {

    private final ServerMemberManager serverMemberManager;

    private final DistroComponentHolder distroComponentHolder;

    private final DistroExecuteTaskExecuteEngine executeTaskExecuteEngine;

    public DistroVerifyTimedTask(ServerMemberManager serverMemberManager, DistroComponentHolder distroComponentHolder,
            DistroExecuteTaskExecuteEngine executeTaskExecuteEngine) {
        this.serverMemberManager = serverMemberManager;
        this.distroComponentHolder = distroComponentHolder;
        this.executeTaskExecuteEngine = executeTaskExecuteEngine;
    }

    @Override
    public void run() {
        try {
            //获取集群中除自已以外的节点
            List<Member> targetServer = serverMemberManager.allMembersWithoutSelf();
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("server list is: {}", targetServer);
            }
            // 每一种类型的数据，都要向其他节点发起验证
            for (String each : distroComponentHolder.getDataStorageTypes()) {
                //执行数据校验请求, 根据类型来验证，这个类型代表着协议类型，2.2.0的版本只会用Grpc的类型
                verifyForDataStorage(each, targetServer);
            }
        } catch (Exception e) {
            Loggers.DISTRO.error("[DISTRO-FAILED] verify task failed.", e);
        }
    }

    private void verifyForDataStorage(String type, List<Member> targetServer) {
        //获取存储源数据 DistroClientDataProcessor
        DistroDataStorage dataStorage = distroComponentHolder.findDataStorage(type);
        //只有当DistroLoadDataTask完成初始化后才能继续，比如加载快照
        // 判断该存储器有没有初始化完成（在启动时会全量拉取数据，拉去完则会设置FinishInitial为true）
        if (!dataStorage.isFinishInitial()) {
            Loggers.DISTRO.warn("data storage {} has not finished initial step, do not send verify data",
                    dataStorage.getClass().getSimpleName());
            return;
        }
        //获取当前节点维护的客户端数据，包含ClientId和版本号
        List<DistroData> verifyData = dataStorage.getVerifyData();
        if (null == verifyData || verifyData.isEmpty()) {
            return;
        }
        //通过任务向集群其它节点发送校验请求
        for (Member member : targetServer) {
            DistroTransportAgent agent = distroComponentHolder.findTransportAgent(type);
            if (null == agent) {
                continue;
            }
            // 获取到distro传输代理对象，依次添加任务到立即执行引擎
            executeTaskExecuteEngine.addTask(member.getAddress() + type,
                    new DistroVerifyExecuteTask(agent, verifyData, member.getAddress(), type));
        }
    }
}
