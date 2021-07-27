package ash.nazg.rest.endpoints;

import ash.nazg.storage.Adapters;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.OutputAdapter;
import ash.nazg.storage.hadoop.HadoopInput;
import ash.nazg.storage.hadoop.HadoopOutput;
import ash.nazg.storage.metadata.AdapterMeta;

import javax.validation.constraints.NotEmpty;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("adapter")
public class AdapterEndpoint {
    @GET
    @Path("input/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public AdapterMeta input(@PathParam("name") @NotEmpty String name) {
        return Adapters.INPUTS.get(name).meta;
    }

    @GET
    @Path("output/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public AdapterMeta output(@PathParam("name") @NotEmpty String name) {
        return Adapters.OUTPUTS.get(name).meta;
    }

    @POST
    @Path("forPath/input")
    @Produces(MediaType.TEXT_PLAIN)
    public String inputForPath(@FormParam("path") @NotEmpty String path) throws Exception {
        InputAdapter inputAdapter = Adapters.inputAdapter(path);
        if (inputAdapter == null) {
            inputAdapter = new HadoopInput();
        }
        return inputAdapter.meta.name;
    }

    @POST
    @Path("forPath/output")
    @Produces(MediaType.TEXT_PLAIN)
    public String outputForPath(@FormParam("path") @NotEmpty String path) throws Exception {
        OutputAdapter outputAdapter = Adapters.outputAdapter(path);
        if (outputAdapter == null) {
            outputAdapter = new HadoopOutput();
        }
        return outputAdapter.meta.name;
    }
}
