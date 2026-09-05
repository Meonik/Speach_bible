/* =====================================================
   SPEECH BIBLE FRONTEND
   ===================================================== */

/* =====================================================
   APPLICATION STATE
   ===================================================== */

let running = false;

let verseSource = null;
let transcriptSource = null;

// Defaults, per spec:
// Sample Rate: 16 kHz | Buffer Duration: 10 sec
// Processing Interval: 4 sec | Whisper Model: base
const DEFAULT_SETTINGS = {
    sample_rate: "16000",
    buffer_duration: "10",
    processing_interval: "4",
    model: "base"
};

/* =====================================================
   DOM SHORTCUTS
   ===================================================== */

const el = (id) => document.getElementById(id);

const sampleRateSelect = () => el("sampleRateSelect");
const bufferDurationSelect = () => el("bufferDurationSelect");
const processingIntervalSelect = () => el("processingIntervalSelect");
const whisperModelSelect = () => el("whisperModelSelect");

/* =====================================================
   SETTINGS HELPERS
   ===================================================== */

function collectSettingsFromUI() {
    return {
        sample_rate: parseInt(sampleRateSelect().value, 10),
        buffer_duration: parseFloat(bufferDurationSelect().value),
        processing_interval: parseFloat(processingIntervalSelect().value),
        model: whisperModelSelect().value
    };
}

function applySettingsToUI(settings) {
    if (settings.sample_rate !== undefined) {
        sampleRateSelect().value = String(settings.sample_rate);
    }
    if (settings.buffer_duration !== undefined) {
        bufferDurationSelect().value = String(parseInt(settings.buffer_duration, 10));
    }
    if (settings.processing_interval !== undefined) {
        processingIntervalSelect().value = String(parseInt(settings.processing_interval, 10));
    }
    if (settings.model !== undefined) {
        whisperModelSelect().value = settings.model;
    }
}

function setSettingsEnabled(enabled) {
    [
        sampleRateSelect(),
        bufferDurationSelect(),
        processingIntervalSelect(),
        whisperModelSelect(),
        el("defaultsButton")
    ].forEach((node) => {
        node.disabled = !enabled;
    });
}

async function fetchInitialSettings() {
    try {
        const res = await fetch("/settings");
        const data = await res.json();
        applySettingsToUI(data);
    } catch (error) {
        console.log("Could not load settings, using page defaults:", error);
    }
}

/* =====================================================
   VERSE STREAM
   ===================================================== */

function connectVerseStream() {

    verseSource = new EventSource("/verse-stream");

    verseSource.onmessage = function (e) {

        const data = e.data;

        /*
         * Current Python format:

           John 3:16 - For God so loved the world...
        */

        const separator = data.indexOf(" - ");

        if (separator !== -1) {

            const reference = data.substring(0, separator);
            const verse = data.substring(separator + 3);
            
            el("reference").innerText = reference;
            el("verseText").innerText = verse;
            el("detectionState").innerText = "DETECTED";
            

        } else {

            el("detectionState").innerText = "WAITING";

        }

    };

    verseSource.onerror = function () {
        console.log("Verse stream connection lost.");
    };

}

/* =====================================================
   LIVE TRANSCRIPTION STREAM
   ===================================================== */

function connectTranscriptStream() {

    transcriptSource = new EventSource("/transcript-stream");

    transcriptSource.onmessage = function (e) {

        const data = e.data;

        if (!data || data === "None" || data.trim() === "") {
            return;
        }

        // The backend streams the latest snapshot of the rolling audio
        // buffer's transcription, so we replace (not append) to get the
        // "rolling text" effect as the buffer moves forward.
        el("transcription").innerText = data;
        el("transcription").scrollTop = el("transcription").scrollHeight;

        if (running) {
            el("transcriptionState").innerText = "TRANSCRIBING";
        }

    };

    transcriptSource.onerror = function () {
        console.log("Transcript stream connection lost.");
    };

}

/* =====================================================
   START
   ===================================================== */

async function startSystem() {

    if (running) {
        return;
    }

    el("startButton").disabled = true;

    // Push whatever is currently selected in the dropdowns to the backend
    // before starting, so the mic/model actually use those values.
    const settings = collectSettingsFromUI();

    try {

        const settingsRes = await fetch("/settings", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(settings)
        });

        const settingsData = await settingsRes.json();

        if (!settingsData.ok) {
            alert(settingsData.error || "Failed to apply settings.");
            el("startButton").disabled = false;
            return;
        }

        const startRes = await fetch("/start", { method: "POST" });
        const startData = await startRes.json();

        if (!startData.ok) {
            alert(startData.error || "Failed to start.");
            el("startButton").disabled = false;
            return;
        }

    } catch (error) {
        console.log("Start error:", error);
        alert("Could not reach the server to start the system.");
        el("startButton").disabled = false;
        return;
    }

    running = true;

    el("statusText").innerText = "LISTENING";
    el("statusDot").classList.add("active");

    el("startButton").disabled = true;
    el("stopButton").disabled = false;
    setSettingsEnabled(false);

    el("transcriptionState").innerText = "LISTENING";
    el("volumeMessage").innerText = "Listening for voice input...";

    console.log("System started.");

}

/* =====================================================
   STOP
   ===================================================== */

async function stopSystem() {

    if (!running) {
        return;
    }

    el("stopButton").disabled = true;

    try {
        await fetch("/stop", { method: "POST" });
    } catch (error) {
        console.log("Stop error:", error);
    }

    running = false;

    el("statusText").innerText = "STOPPED";
    el("statusDot").classList.remove("active");

    el("startButton").disabled = false;
    el("stopButton").disabled = true;
    setSettingsEnabled(true);

    el("transcriptionState").innerText = "NO AUDIO";
    el("volumeMessage").innerText = "Microphone stopped.";

    el("volumeLevel").style.width = "0%";
    el("volumePercentage").innerText = "0%";

    console.log("System stopped.");

}

/* =====================================================
   RESTORE DEFAULTS
   ===================================================== */

function restoreDefaults() {

    if (running) {
        return;
    }

    applySettingsToUI(DEFAULT_SETTINGS);

}

/* =====================================================
   STATUS POLLING (volume, cpu, response time)
   ===================================================== */

function formatResponseTime(seconds) {
    if (!seconds || seconds <= 0) {
        return "--";
    }
    return seconds.toFixed(2) + " s";
}

async function updateStatus() {

    try {

        const response = await fetch("/status");
        const data = await response.json();

        const volume = Math.round(data.volume || 0);

        el("volumeLevel").style.width = volume + "%";
        el("volumePercentage").innerText = volume + "%";

        el("cpuUsage").innerText = Math.round(data.cpu || 0) + "%";

        el("responseTime").innerText = formatResponseTime(data.response_time);

        // Keep the dashboard in sync in case the backend's running state
        // ever drifts from what the buttons think (e.g. mic error).
        if (data.running !== running) {
            running = data.running;

            if (running) {
                el("statusText").innerText = "LISTENING";
                el("statusDot").classList.add("active");
                el("startButton").disabled = true;
                el("stopButton").disabled = false;
                setSettingsEnabled(false);
            } else {
                el("statusText").innerText = "STOPPED";
                el("statusDot").classList.remove("active");
                el("startButton").disabled = false;
                el("stopButton").disabled = true;
                setSettingsEnabled(true);
                el("transcriptionState").innerText = "NO AUDIO";
            }
        }

    } catch (error) {
        console.log("Status update error:", error);
    }

}

/* =====================================================
   STATUS UPDATE LOOP
   ===================================================== */

setInterval(updateStatus, 250);

/* =====================================================
   INIT
   ===================================================== */

document.addEventListener("DOMContentLoaded", function () {

    el("startButton").addEventListener("click", startSystem);
    el("stopButton").addEventListener("click", stopSystem);
    el("defaultsButton").addEventListener("click", restoreDefaults);

    fetchInitialSettings();
    connectVerseStream();
    connectTranscriptStream();
    updateStatus();

});