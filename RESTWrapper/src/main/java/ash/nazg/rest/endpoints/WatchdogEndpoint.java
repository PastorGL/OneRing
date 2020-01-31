/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.endpoints;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("")
public class WatchdogEndpoint {
    @GET
    @Path("alive")
    @Produces(MediaType.TEXT_PLAIN)
    public String status() {
        return "Ok";
    }
}
