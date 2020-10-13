package mediathek.filmlisten.writer;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import mediathek.config.Daten;
import mediathek.daten.DatenFilm;
import mediathek.daten.FilmResolution;
import mediathek.daten.ListeFilme;
import mediathek.gui.messages.FilmListWriteStartEvent;
import mediathek.gui.messages.FilmListWriteStopEvent;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class CBORFilmListWriter {

    private static final Logger logger = LogManager.getLogger(CBORFilmListWriter.class);
    //private final CBORFactory factory = new CBORFactory();
    private final JsonFactory factory = new JsonFactory();

    private void checkOsxCacheDirectory() {
        final Path filePath = Paths.get(System.getProperty("user.home") + File.separator + "Library/Caches/MediathekView");
        if (Files.notExists(filePath)) {
            try {
                Files.createDirectories(filePath);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private void writeFormatHeader(JsonGenerator jg, ListeFilme listeFilme) throws IOException {
        final var meta = listeFilme.metaData();

        jg.writeObjectFieldStart("mtdt");
        jg.writeNumberField("version", Integer.parseInt(meta.getVersion()));
        jg.writeBinaryField("id", meta.getId().getBytes());
        jg.writeStringField("created", meta.getDatum());
        jg.writeEndObject();
    }

    public void writeFilmList(String datei, ListeFilme listeFilme, IProgressListener listener) {
        Daten.getInstance().getMessageBus().publishAsync(new FilmListWriteStartEvent());

        try {
            logger.info("Filme schreiben ({} Filme) :", listeFilme.size());
            logger.info("   --> Start Schreiben nach: {}", datei);

            //Check if Cache directory exists on OSX
            if (SystemUtils.IS_OS_MAC_OSX) {
                checkOsxCacheDirectory();
            }


            Path filePath = Paths.get(datei);
            try {
                Files.deleteIfExists(filePath);
            }
            catch (Exception e) {
                logger.error("writeFilmList", e);
            }
            long start = System.nanoTime();

            try (OutputStream fos = Files.newOutputStream(filePath);
                 BufferedOutputStream bos = new BufferedOutputStream(fos, 64 * 1024);
                 JsonGenerator jg = factory.createGenerator(bos, JsonEncoding.UTF8)) {

                jg.useDefaultPrettyPrinter();
                jg.writeStartObject();

                writeFormatHeader(jg, listeFilme);

                final long filmEntries = listeFilme.size();
                float curEntry = 0f;

                for (DatenFilm datenFilm : listeFilme) {
                    writeEntry(datenFilm, jg);
                    if (listener != null) {
                        listener.progress(curEntry / filmEntries);
                        curEntry++;
                    }
                }
                jg.writeEndObject();

                if (listener != null)
                    listener.progress(1d);

                long end = System.nanoTime();

                logger.info("   --> geschrieben!");
                logger.info("Write duration: {} ms", TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS));
            }
        } catch (Exception ex) {
            logger.error("nach: {}", datei, ex);
        }

        Daten.getInstance().getMessageBus().publishAsync(new FilmListWriteStopEvent());
    }

    private static final long EMPTY_VALUE = Long.MAX_VALUE;

    private void writeEntry(DatenFilm entry, JsonGenerator jg) throws IOException {
        jg.writeObjectFieldStart("film");

        jg.writeStringField("sender", entry.getSender());
        jg.writeStringField("thema", entry.getThema());
        jg.writeStringField("title", entry.getTitle());
        jg.writeStringField("date", entry.getSendeDatum());
        writeZeit(jg, entry);

        var dauer = entry.getDauer();
        long duration = EMPTY_VALUE;
        if (!dauer.isEmpty())
            duration = entry.getDuration();
        jg.writeNumberField("duration", duration);

        var size = entry.getSize();
        long sizel = EMPTY_VALUE;
        if (!size.isEmpty())
            sizel = Long.parseLong(size);
        jg.writeNumberField("size", sizel);

        jg.writeStringField("description", entry.getDescription());
        jg.writeStringField("website", entry.getWebsiteLink());
        jg.writeStringField("url_subtitle", entry.getUrlSubtitle());
        jg.writeStringField("url", entry.getUrl());
        jg.writeStringField("url_klein", entry.getUrlFuerAufloesung(FilmResolution.AUFLOESUNG_KLEIN));
        jg.writeStringField("url_hq", entry.getUrlFuerAufloesung(FilmResolution.AUFLOESUNG_HD));

        var datum = entry.getDatumLong();
        long datuml = EMPTY_VALUE;
        if (!datum.isEmpty())
            datuml = Long.parseLong(datum);
        jg.writeNumberField("datum_long", datuml);

        jg.writeStringField("geo", entry.getGeo().orElse(""));
        jg.writeBooleanField("new", entry.isNew());

        jg.writeEndObject();
    }

    private void writeZeit(JsonGenerator jg, DatenFilm datenFilm) throws IOException {
        String strZeit = datenFilm.getSendeZeit();
        final int len = strZeit.length();

        if (strZeit.isEmpty() || len < 8)
            jg.writeStringField("time","");
        else {
            strZeit = strZeit.substring(0, len - 3);
            jg.writeStringField("time", strZeit);
        }
    }

    @FunctionalInterface
    public interface IProgressListener {
        void progress(double current);
    }
}
