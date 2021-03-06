package com.google.cloud.teleport.v2.neo4j.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileSystems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for reading local and remote file resources.
 */
public class FileSystemUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtils.class);

    public static String getPathContents(String gsPath) {
        StringBuilder textBuilder = new StringBuilder();

        try {
            ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(
                    gsPath
                    , false /* is_directory */
            ));
            InputStream inputStream = Channels.newInputStream(chan);
            // Use regular Java utilities to work with the input stream.

            try (Reader reader = new BufferedReader(new InputStreamReader
                    (inputStream, Charset.forName(StandardCharsets.UTF_8.name())))) {
                int c = 0;
                while ((c = reader.read()) != -1) {
                    textBuilder.append((char) c);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return textBuilder.toString();
    }

    public static String getAbsoluteResourcePath(String resourceName) {
        ClassLoader classLoader = FileSystemUtils.class.getClass().getClassLoader();
        File file = new File(classLoader.getResource(resourceName).getFile());
        return file.getAbsolutePath();
    }
}
