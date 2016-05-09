package org.apache.samoa;

import com.datatorrent.api.StreamingApplication;

import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * Created by pramod on 4/5/16.
 */
class StreamingAppFactory implements StramAppLauncher.AppFactory {
    private StreamingApplication app;
    private String name;

    public StreamingAppFactory(StreamingApplication app, String name) {
        this.app = app;
        this.name = name;
    }

    public LogicalPlan createApp(LogicalPlanConfiguration planConfig) {
        LogicalPlan dag = new LogicalPlan();
        planConfig.prepareDAG(dag, app, getName());
        return dag;
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return name;
    }
}