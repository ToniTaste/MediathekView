package mediathek.filmlisten.writer;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import mediathek.config.Daten;
import mediathek.daten.DatenFilm;
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
    private static final String TAG_JSON_LIST = "X";
    private String sender = "";
    private String thema = "";
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

            sender = "";
            thema = "";

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
        jg.writeArrayFieldStart(TAG_JSON_LIST);

        writeSender(jg, entry);
        writeThema(jg, entry);
        writeTitel(jg, entry);
        jg.writeString(entry.getSendeDatum());
        writeZeit(jg, entry);

        var dauer = entry.getDauer();
        if (!dauer.isEmpty())
            jg.writeNumber(entry.getDuration());
        else
            jg.writeNumber(EMPTY_VALUE);

        var size = entry.getSize();
        long sizel = EMPTY_VALUE;
        if (!size.isEmpty())
            sizel = Long.parseLong(size);
        jg.writeNumber(sizel);

        jg.writeString(entry.getDescription());
        jg.writeString(entry.getUrl());
        jg.writeString(entry.getWebsiteLink());
        jg.writeString(entry.getUrlSubtitle());
        jg.writeString(entry.getUrlKlein());
        jg.writeString(entry.getHighQualityUrl());

        var datum = entry.getDatumLong();
        long datuml = EMPTY_VALUE;
        if (!datum.isEmpty())
            datuml = Long.parseLong(datum);
        jg.writeNumber(datuml);

        jg.writeString(entry.getGeo().orElse(""));
        jg.writeBoolean(entry.isNew());

        jg.writeEndArray();
    }

    private void writeTitel(JsonGenerator jg, DatenFilm datenFilm) throws IOException {
        jg.writeString(datenFilm.getTitle());
    }

    private void writeSender(JsonGenerator jg, DatenFilm datenFilm) throws IOException {
        String tempSender = datenFilm.getSender();

        if (tempSender.equals(sender)) {
            jg.writeString("");
        } else {
            sender = tempSender;
            jg.writeString(tempSender);
        }
    }

    private void writeThema(JsonGenerator jg, DatenFilm datenFilm) throws IOException {
        if (datenFilm.getThema().equals(thema)) {
            jg.writeString("");
        } else {
            thema = datenFilm.getThema();
            jg.writeString(datenFilm.getThema());
        }
    }

    private void writeZeit(JsonGenerator jg, DatenFilm datenFilm) throws IOException {
        String strZeit = datenFilm.getSendeZeit();
        final int len = strZeit.length();

        if (strZeit.isEmpty() || len < 8)
            jg.writeString("");
        else {
            strZeit = strZeit.substring(0, len - 3);
            jg.writeString(strZeit);
        }
    }

    @FunctionalInterface
    public interface IProgressListener {
        void progress(double current);
    }
}
