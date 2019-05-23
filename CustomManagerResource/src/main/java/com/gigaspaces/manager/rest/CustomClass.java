package com.gigaspaces.manager.rest;

import org.openspaces.admin.rest.*;


@CustomManagerResource
@Path("/demo")

public class CustomManagerResource {
    @Context Admin admin;

    @GET
    @Path("/report")
    public String report(@QueryParam("hostname") String hostname) {
        Machine machine = admin.getMachines().getMachineByHostName(hostname);
        return "Custom report: host=" + hostname +
                ", containers=" + machine.getGridServiceContainers() +
                ", PU instances=" + machine.getProcessingUnitInstances();
    }
}
