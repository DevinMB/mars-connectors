package io.mars.connector.sdk;

import java.nio.file.Path;
import java.util.ServiceLoader;

/**
 * Build-time entry point: discovers the {@link MarsConnector} on the classpath and
 * writes its manifest into the module's output directory, so the JSON that ships in
 * the JAR is always generated from the same Java declaration the connector uses.
 *
 * <p>Invoked from Maven during {@code prepare-package} with the output directory as
 * the single argument.
 */
public final class ManifestGenerator {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: ManifestGenerator <output-dir>");
        }
        Path out = Path.of(args[0]);

        int found = 0;
        for (MarsConnector c : ServiceLoader.load(MarsConnector.class, ManifestGenerator.class.getClassLoader())) {
            ConnectorManifest m = c.manifest();
            ManifestWriter.write(m, out);
            System.out.println("[mars] wrote manifest for '" + m.connector()
                    + "' (" + m.options().size() + " options) to " + out.resolve(ManifestWriter.RESOURCE_NAME));
            found++;
        }

        if (found == 0) {
            throw new IllegalStateException(
                    "no MarsConnector found on the classpath -- is "
                    + "META-INF/services/io.mars.connector.sdk.MarsConnector missing?");
        }
        if (found > 1) {
            throw new IllegalStateException(
                    "found " + found + " MarsConnector implementations; expected exactly one per module");
        }
    }
}
