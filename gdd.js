// Register Chart.js plugins
Chart.register(ChartDataLabels);

// Register zoom plugin if available
if (typeof ChartZoom !== 'undefined') {
    Chart.register(ChartZoom);
} else {
    console.warn("ChartZoom plugin not found, zoom features will be disabled.");
}

// Global auto-refresh settings
let autoRefresh = true;
let refreshIntervalId = null;
const AUTO_REFRESH_PERIOD = 60000; // 60 seconds

// Sample event flags for years (modify as needed)
const yearEvents = {
    "2000": ["Neutral", "Solar Max [8]"],
    "2001": ["Neutral", "Solar Max [8]"],
    "2002": ["Neutral", "Solar Transition [7]"],
    "2003": ["El Niño", "Solar Transition [6]"],
    "2004": ["Neutral", "Solar Transition [5]"],
    "2005": ["Neutral", "Solar Transition [4]"],
    "2006": ["Neutral", "Solar Transition [3]"],
    "2007": ["El Niño", "Solar Transition [2]"],
    "2008": ["La Niña", "Solar Min [1]"],
    "2009": ["La Niña", "Solar Min [1]"],
    "2010": ["La Niña", "Solar Transition [2]"],
    "2011": ["La Niña", "Solar Transition [3]"],
    "2012": ["La Niña", "Solar Transition [4]"],
    "2013": ["Neutral", "Solar Transition [4]"],
    "2014": ["Neutral", "Solar Max [6]"],
    "2015": ["El Niño", "Solar Transition [5]"],
    "2016": ["El Niño", "Solar Transition [4]"],
    "2017": ["La Niña", "Solar Transition [3]"],
    "2018": ["Neutral", "Solar Min [2]"],
    "2019": ["Weak El Niño", "Solar Min [1]"],
    "2020": ["Neutral", "Solar Min [1]"],
    "2021": ["La Niña", "Solar Transition [2]"],
    "2022": ["Moderate La Niña", "Solar Transition [3]"],
    "2023": ["El Niño", "Solar Transition [4]"],
    "2024": ["Transition from El Niño to Neutral", "Solar Transition [6]"],
    "2025": ["Neutral", "Solar Max [10]"]
};

// Custom Plugin: horizontalLinePlugin draws horizontal lines at 32°F, 50°F, and 86°F
const horizontalLinePlugin = {
    id: 'horizontalLinePlugin',
    afterDraw(chart, args, options) {
        const {ctx, chartArea: {left, right}, scales: {y}} = chart;
        if (!y) return;

        function drawLine(yValue, color, text) {
            const yPixel = y.getPixelForValue(yValue);
            ctx.save();
            ctx.beginPath();
            ctx.setLineDash([6, 6]);
            ctx.strokeStyle = color;
            ctx.lineWidth = 2;
            ctx.moveTo(left, yPixel);
            ctx.lineTo(right, yPixel);
            ctx.stroke();
            ctx.restore();
            ctx.fillStyle = color;
            ctx.font = '12px Arial';
            ctx.fillText(text, left + 5, yPixel - 5);
        }

        drawLine(32, 'green', 'Freeze (32°F)');
        drawLine(50, 'blue', 'Base Temp (50°F)');
        drawLine(86, 'red', 'Ceiling Temp (86°F)');
    }
};
Chart.register(horizontalLinePlugin);

// Custom Plugin: verticalLinePlugin (supports dash patterns)
// Removed dependency on current year selection so that lines always draw
const verticalLinePlugin = {
    id: 'verticalLinePlugin',
    afterDraw(chart, args, options) {
        const ctx = chart.ctx;
        const xScale = chart.scales.x;
        if (!options.lines) return;
        options.lines.forEach(line => {
            const xPixel = xScale.getPixelForValue(line.xLabel);
            if (xPixel === undefined || isNaN(xPixel)) return;
            ctx.save();
            if (line.dashPattern && line.dashPattern.length > 0) {
                ctx.setLineDash(line.dashPattern);
            } else {
                ctx.setLineDash([]);
            }
            ctx.beginPath();
            ctx.moveTo(xPixel, chart.chartArea.top);
            ctx.lineTo(xPixel, chart.chartArea.bottom);
            ctx.lineWidth = line.lineWidth || 2;
            ctx.strokeStyle = line.color || "red";
            ctx.stroke();
            ctx.restore();
        });
    }
};
Chart.register(verticalLinePlugin);

// Global variables and configuration
let db = null;
let dailyChart;
let SQL;
let selectedYears = []; // Global array for selected years
let yearColorMapping = {}; // Global persistent color mapping

// Declare DEBUG before any usage
let DEBUG = true;

function log(message) {
    const now = new Date().toISOString().replace("T", " ").replace("Z", "");
    console.log("[" + now + "] " + message);
}

function log_debug(message) {
    if (DEBUG) {
        log("[DEBUG] " + message);
    }
}

// Database filename
let DB_FILENAME = "ambient_weather.sqlite";

// Helper: Hash function for deterministic fallback color generation
function hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        hash += str.charCodeAt(i);
    }
    return hash;
}

function generateDistinctColor(index, total) {
    const hue = Math.floor((360 / total) * index);
    return `hsl(${hue}, 70%, 50%)`;
}

// Use localStorage (instead of sessionStorage) for persistent yearColorMapping
function populateYearControls() {
    let storedMapping = localStorage.getItem("yearColorMapping");
    if (storedMapping) {
        yearColorMapping = JSON.parse(storedMapping);
    } else {
        yearColorMapping = {};
    }
    const res = db.exec("SELECT DISTINCT substr(date, 1, 4) as year FROM readings ORDER BY year ASC;");
    const yearControls = document.getElementById("yearControls");
    if (!yearControls) return;
    yearControls.innerHTML = "";
    const allYearsLabel = document.createElement("label");
    allYearsLabel.innerHTML = `<input type="checkbox" id="allYears" checked> All Years`;
    yearControls.appendChild(allYearsLabel);
    if (res.length > 0) {
        const years = res[0].values.map(row => row[0]);
        const totalYears = years.length;
        years.forEach((year, index) => {
            let labelText = year;
            if (yearEvents.hasOwnProperty(year) && yearEvents[year].length > 0) {
                labelText += " (" + yearEvents[year].join(", ") + ")";
            }
            if (!yearColorMapping[year]) {
                yearColorMapping[year] = generateDistinctColor(index, totalYears);
            }
            localStorage.setItem("yearColorMapping", JSON.stringify(yearColorMapping));
            const label = document.createElement("label");
            label.innerHTML = `<input type="checkbox" name="year" value="${year}" checked>
             <span style="display:inline-block; width:12px; height:12px; background-color:${yearColorMapping[year]}; margin-right:5px;"></span>
             ${labelText}`;
            yearControls.appendChild(label);
        });
    }
}

// Updated SQL query functions using parameter binding
function queryDataByYearAndMonths(year, months) {
    const placeholders = months.map(() => '?').join(',');
    const sqlQuery = `
        SELECT substr(date, 6, 5) AS day_key,
               MIN(tempf)         AS minTempF,
               MAX(tempf)         AS maxTempF,
               AVG(tempf)         AS avgTempF,
               MAX(gdd)           AS maxCumGDD,
               MAX(dailyrainin)   AS rainTotal
        FROM readings
        WHERE substr(date, 1, 4) = ?
          AND cast(substr(date, 6, 2) as integer) IN (${placeholders})
        GROUP BY day_key
        ORDER BY day_key ASC;
    `;
    const stmt = db.prepare(sqlQuery);
    stmt.bind([year, ...months]);
    const results = [];
    while (stmt.step()) {
        results.push(stmt.getAsObject());
    }
    stmt.free();
    return results;
}

function querySunspotsDataByYearAndMonths(year, months) {
    const placeholders = months.map(() => '?').join(',');
    const sqlQuery = `
        SELECT substr(date, 6, 5) AS day_key,
               daily_total        AS sunspotCount
        FROM sunspots
        WHERE substr(date, 1, 4) = ?
          AND cast(substr(date, 6, 2) as integer) IN (${placeholders})
        ORDER BY day_key ASC;
    `;
    const stmt = db.prepare(sqlQuery);
    stmt.bind([year, ...months]);
    const results = {};
    while (stmt.step()) {
        const row = stmt.getAsObject();
        results[row.day_key] = row.sunspotCount;
    }
    stmt.free();
    return results;
}

function processYearlyData(rows) {
    const data = {};
    rows.forEach(r => {
        data[r.day_key] = {
            minTempF: parseFloat(r.minTempF),
            maxTempF: parseFloat(r.maxTempF),
            avgTempF: parseFloat(r.avgTempF),
            maxCumGDD: parseFloat(r.maxCumGDD),
            rainTotal: parseFloat(r.rainTotal)
        };
    });
    return data;
}

function alignData(commonLabels, dataByDay, field) {
    return commonLabels.map(label => dataByDay[label] ? dataByDay[label][field] : null);
}

// Compute a simple linear regression trend line
// Added check for zero denominator to avoid division by zero
function computeTrendLine(data) {
    let x = [], y = [];
    for (let i = 0; i < data.length; i++) {
        if (data[i] !== null && !isNaN(data[i])) {
            x.push(i);
            y.push(data[i]);
        }
    }
    if (x.length === 0) {
        return new Array(data.length).fill(null);
    }
    let n = x.length;
    let sumX = x.reduce((a, b) => a + b, 0);
    let sumY = y.reduce((a, b) => a + b, 0);
    let sumXY = 0, sumXX = 0;
    for (let i = 0; i < n; i++) {
        sumXY += x[i] * y[i];
        sumXX += x[i] * x[i];
    }
    let denominator = n * sumXX - sumX * sumX;
    if (denominator === 0) {
        return new Array(data.length).fill(null);
    }
    let slope = (n * sumXY - sumX * sumY) / denominator;
    let intercept = (sumY - slope * sumX) / n;
    let trend = [];
    for (let i = 0; i < data.length; i++) {
        trend.push(slope * i + intercept);
    }
    return trend;
}

function refreshData() {
    const modeSelect = document.getElementById("modeSelect");
    if (!modeSelect) return;
    const mode = modeSelect.value;
    if (mode === "nearest") {
        applyNearestNeighborMode();
    } else if (mode === "greater") {
        applyGDDGreaterMode();
    } else if (mode === "less") {
        applyGDDLessMode();
    } else {
        loadAndPlotData();
    }
}

function startAutoRefresh() {
    if (!refreshIntervalId) {
        refreshIntervalId = setInterval(refreshData, AUTO_REFRESH_PERIOD);
        log("Auto Refresh started (every " + (AUTO_REFRESH_PERIOD / 1000) + " seconds).");
    }
}

function stopAutoRefresh() {
    if (refreshIntervalId) {
        clearInterval(refreshIntervalId);
        refreshIntervalId = null;
        log("Auto Refresh stopped.");
    }
}

function getLegendCategory(label) {
    if (label.includes("Trend")) return "Avg Temp Trend";
    if (label.includes("Min Temp") || label.includes("Max Temp")) return "Min/Max Temp";
    if (label.includes("Avg Temp")) return "Avg Temperature";
    if (label.includes("Cumulative GDD")) return "Cumulative GDD";
    if (label.includes("Daily Rain Total")) return "Daily Rain Totals";
    if (label.includes("Sunspots Daily Total")) return "Sunspot Daily Total";
    if (label.includes("Pest")) return "Pest Spray";
    return "Grape Varietal";
}

// Initialize sql.js and load the database
initSqlJs({locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.12.0/${file}`})
    .then(function (SQLLib) {
        SQL = SQLLib;
        return fetch(DB_FILENAME);
    })
    .then(response => response.arrayBuffer())
    .then(buffer => {
        db = new SQL.Database(new Uint8Array(buffer));
        log("Database loaded successfully.");
        populateYearControls();
        loadFilterSettings();
        refreshData();
        startAutoRefresh();

        // Event listeners for year checkboxes
        document.querySelectorAll("input[name='year']").forEach(cb => {
            cb.addEventListener("change", function () {
                let allChecked = true;
                document.querySelectorAll("input[name='year']").forEach(box => {
                    if (!box.checked) {
                        allChecked = false;
                    }
                });
                const allYearsCb = document.getElementById("allYears");
                if (allYearsCb) allYearsCb.checked = allChecked;
                saveFilterSettings();
                loadAndPlotData();
            });
        });

        // Event listener for "All Years" checkbox
        const allYearsCb = document.getElementById("allYears");
        if (allYearsCb) {
            allYearsCb.addEventListener("change", function () {
                const newState = this.checked;
                document.querySelectorAll("input[name='year']").forEach(cb => {
                    cb.checked = newState;
                });
                saveFilterSettings();
                loadAndPlotData();
            });
        }

        // Event listeners for month checkboxes
        document.querySelectorAll("input[name='month']").forEach(cb => {
            cb.addEventListener("change", function () {
                let allChecked = true;
                document.querySelectorAll("input[name='month']").forEach(box => {
                    if (!box.checked) {
                        allChecked = false;
                    }
                });
                const allMonthsCb = document.getElementById("allMonths");
                if (allMonthsCb) allMonthsCb.checked = allChecked;
                saveFilterSettings();
                loadAndPlotData();
            });
        });

        // Event listener for "All Months" checkbox
        const allMonthsCb = document.getElementById("allMonths");
        if (allMonthsCb) {
            allMonthsCb.addEventListener("change", function () {
                const newState = this.checked;
                document.querySelectorAll("input[name='month']").forEach(cb => {
                    cb.checked = newState;
                });
                saveFilterSettings();
                loadAndPlotData();
            });
        }

        // Event listener for auto-refresh checkbox
        const autoRefreshCheckbox = document.getElementById("autoRefreshCheckbox");
        if (autoRefreshCheckbox) {
            autoRefreshCheckbox.addEventListener("change", function () {
                autoRefresh = this.checked;
                log("Auto Refresh is now " + (autoRefresh ? "enabled" : "disabled"));
                if (autoRefresh) {
                    startAutoRefresh();
                } else {
                    stopAutoRefresh();
                }
            });
        }

        // Event listener for model selection dropdown
        const modelSelect = document.getElementById("modelSelect");
        if (modelSelect) {
            modelSelect.addEventListener("change", function () {
                saveFilterSettings();
                loadAndPlotData();
            });
        }
    })
    .catch(err => console.error("Error initializing database:", err));

function saveFilterSettings() {
    const monthCheckboxes = document.querySelectorAll("input[name='month']");
    const selectedMonths = Array.from(monthCheckboxes)
        .filter(cb => cb.checked)
        .map(cb => cb.value);
    localStorage.setItem("selectedMonths", JSON.stringify(selectedMonths));
    const yearCheckboxes = document.querySelectorAll("input[name='year']");
    const selYears = Array.from(yearCheckboxes)
        .filter(cb => cb.checked)
        .map(cb => cb.value);
    localStorage.setItem("selectedYears", JSON.stringify(selYears));
    const modelSelect = document.getElementById("modelSelect");
    if (modelSelect) {
        localStorage.setItem("selectedModel", modelSelect.value);
    }
}

function loadFilterSettings() {
    const storedMonths = localStorage.getItem("selectedMonths");
    if (storedMonths) {
        const selectedMonths = JSON.parse(storedMonths);
        document.querySelectorAll("input[name='month']").forEach(cb => {
            cb.checked = selectedMonths.includes(cb.value);
        });
        const allChecked = Array.from(document.querySelectorAll("input[name='month']")).every(cb => cb.checked);
        const allMonthsCb = document.getElementById("allMonths");
        if (allMonthsCb) allMonthsCb.checked = allChecked;
    }
    const storedYears = localStorage.getItem("selectedYears");
    if (storedYears) {
        selectedYears = JSON.parse(storedYears);
        document.querySelectorAll("input[name='year']").forEach(cb => {
            cb.checked = selectedYears.includes(cb.value);
        });
        const allChecked = Array.from(document.querySelectorAll("input[name='year']")).every(cb => cb.checked);
        const allYearsCb = document.getElementById("allYears");
        if (allYearsCb) allYearsCb.checked = allChecked;
    }
    const storedModel = localStorage.getItem("selectedModel");
    const modelSelect = document.getElementById("modelSelect");
    if (storedModel && modelSelect) {
        modelSelect.value = storedModel;
    }
}

// Compute Bud Break Points for grape varieties
function computeBudBreakPoints(selectedYearsArr, commonLabels, yearlyData) {
    const result = db.exec("SELECT variety, heat_summation FROM grapevine_gdd ORDER BY variety");
    if (result.length === 0) return [];
    const grapeData = result[0].values;
    const budBreakByVariety = {};
    grapeData.forEach(row => {
        budBreakByVariety[row[0]] = [];
    });
    selectedYearsArr.forEach(year => {
        const dataForYear = yearlyData[year];
        grapeData.forEach(row => {
            const variety = row[0];
            const threshold = row[1];
            for (let i = 0; i < commonLabels.length; i++) {
                const dayKey = commonLabels[i];
                const record = dataForYear[dayKey];
                if (record && record.maxCumGDD >= threshold) {
                    budBreakByVariety[variety].push({x: dayKey, y: record.maxCumGDD, year: year});
                    break;
                }
            }
        });
    });
    const scatterDatasets = [];
    Object.keys(budBreakByVariety).forEach(variety => {
        const points = budBreakByVariety[variety];
        if (points.length > 0) {
            scatterDatasets.push({
                label: variety,
                data: points,
                borderColor: getColorForVariety(variety),
                backgroundColor: getColorForVariety(variety),
                pointRadius: 6,
                type: 'scatter',
                showLine: false,
                order: 2,
                yAxisID: 'y1',
                spanGaps: false
            });
        }
    });
    return scatterDatasets;
}

// Compute Pest Spray Points – plot only the min_gdd points for the current year
function computePestSprayPoints(selectedYearsArr, commonLabels, yearlyData) {
    const currentYear = String(new Date().getFullYear());
    if (!selectedYearsArr.includes(currentYear)) return [];
    const result = db.exec("SELECT common_name, scientific_name, stage, min_gdd, max_gdd, dormant FROM vineyard_pests ORDER BY common_name, min_gdd");
    if (!result || result.length === 0) return [];
    const pestData = result[0].values;
    const pestPointsMap = {};
    pestData.forEach(row => {
        const pest = row[0];
        const scientificName = row[1];
        const stage = row[2];
        const minThreshold = row[3];
        const maxThreshold = row[4];
        const dormant = row[5];
        const dataForYear = yearlyData[currentYear];
        if (!dataForYear) return;
        let point = null;
        for (let i = 0; i < commonLabels.length; i++) {
            const dayKey = commonLabels[i];
            const record = dataForYear[dayKey];
            if (record && record.maxCumGDD >= minThreshold) {
                point = {
                    x: dayKey,
                    y: minThreshold,
                    year: currentYear,
                    scientific_name: scientificName,
                    stage: stage,
                    max_gdd: maxThreshold,
                    dormant: dormant
                };
                break;
            }
        }
        if (point) {
            if (!pestPointsMap[pest]) pestPointsMap[pest] = [];
            pestPointsMap[pest].push(point);
        }
    });
    const pestDatasets = [];
    Object.keys(pestPointsMap).forEach(pest => {
        pestDatasets.push({
            label: pest,
            data: pestPointsMap[pest],
            pointStyle: 'rect',
            pointRadius: 6,
            backgroundColor: getColorForVariety(pest),
            borderColor: getColorForVariety(pest),
            type: 'scatter',
            showLine: false,
            order: 8,
            yAxisID: 'y1',
            hideInLegend: true
        });
    });
    return pestDatasets;
}

function getColorForVariety(variety) {
    const grapeColors = {
        "Chardonnay": "rgb(255,140,0)",
        "Sauvignon Blanc": "rgb(255,215,0)",
        "Riesling": "rgb(255,69,0)",
        "Pinot Gris": "rgb(30,144,255)",
        "Semillon": "rgb(255,99,71)",
        "Cabernet Sauvignon": "rgb(139,0,0)",
        "Merlot": "rgb(220,20,60)",
        "Pinot Noir": "rgb(128,0,0)",
        "Syrah": "rgb(178,34,34)",
        "Zinfandel": "rgb(138,43,226)",
        "Sangiovese": "rgb(165,42,42)",
        "Tempranillo": "rgb(199,21,133)",
        "Malbec": "rgb(128,0,128)",
        "Grenache": "rgb(255,105,180)",
        "Nebbiolo": "rgb(75,0,130)"
    };
    if (grapeColors[variety]) return grapeColors[variety];
    // Deterministic fallback using a hash
    let hue = hashString(variety) % 360;
    return `hsl(${hue}, 70%, 50%)`;
}

function randomColor() {
    const r = Math.floor(Math.random() * 156) + 100;
    const g = Math.floor(Math.random() * 156) + 100;
    const b = Math.floor(Math.random() * 156) + 100;
    return `rgb(${r},${g},${b})`;
}

function darkenColor(rgbStr, factor) {
    const result = /rgb\((\d+),\s*(\d+),\s*(\d+)\)/.exec(rgbStr);
    if (!result) return rgbStr;
    let r = Math.floor(parseInt(result[1]) * factor);
    let g = Math.floor(parseInt(result[2]) * factor);
    let b = Math.floor(parseInt(result[3]) * factor);
    return `rgb(${r},${g},${b})`;
}

function lightenColor(rgbStr, factor) {
    const result = /rgb\((\d+),\s*(\d+),\s*(\d+)\)/.exec(rgbStr);
    if (!result) return rgbStr;
    let r = parseInt(result[1]);
    let g = parseInt(result[2]);
    let b = parseInt(result[3]);
    r = Math.floor(r + (255 - r) * factor);
    g = Math.floor(g + (255 - g) * factor);
    b = Math.floor(b + (255 - b) * factor);
    return `rgb(${r},${g},${b})`;
}

function getLegendGroupStates() {
    const stored = localStorage.getItem("legendGroupStates");
    return stored ? JSON.parse(stored) : {};
}

function saveLegendGroupStates(states) {
    localStorage.setItem("legendGroupStates", JSON.stringify(states));
}

function generateCustomLegend(chart) {
    const legendContainer = document.getElementById('legendContainer');
    if (!legendContainer) return;
    legendContainer.innerHTML = '';
    const grapeVarietalContainer = document.getElementById('grapeVarietalLegend');
    if (grapeVarietalContainer) grapeVarietalContainer.innerHTML = '';
    const groups = {};
    chart.data.datasets.forEach((ds, index) => {
        if (ds.hideInLegend) return;
        const cat = getLegendCategory(ds.label);
        if (!groups[cat]) groups[cat] = [];
        groups[cat].push({ds, index});
    });
    let legendGroupStates = getLegendGroupStates();
    Object.keys(groups).forEach(category => {
        if (category === "Grape Varietal") {
            const header = document.createElement('div');
            header.textContent = category;
            header.style.fontWeight = 'bold';
            header.style.marginTop = '10px';
            header.style.textAlign = 'left';
            if (grapeVarietalContainer) grapeVarietalContainer.appendChild(header);
            const groupDiv = document.createElement('div');
            groupDiv.style.display = 'flex';
            groupDiv.style.flexDirection = 'column';
            groupDiv.style.gap = '8px';
            groups[category].forEach(item => {
                const {ds, index} = item;
                const legendItem = document.createElement('div');
                legendItem.className = 'legend-item';
                legendItem.style.opacity = chart.data.datasets[index].hidden ? 0.5 : 1;
                let markerHTML = `<span style="display:inline-block; width:12px; height:12px; background-color:${ds.borderColor}; border-radius:50%; margin-right:5px;"></span>`;
                legendItem.innerHTML = markerHTML + ds.label;
                legendItem.style.cursor = 'pointer';
                legendItem.onclick = function () {
                    chart.data.datasets[index].hidden = !chart.data.datasets[index].hidden;
                    legendItem.style.opacity = chart.data.datasets[index].hidden ? 0.5 : 1;
                    chart.update();
                };
                groupDiv.appendChild(legendItem);
            });
            if (grapeVarietalContainer) grapeVarietalContainer.appendChild(groupDiv);
        } else {
            const groupWrapper = document.createElement('div');
            groupWrapper.style.display = "inline-flex";
            groupWrapper.style.flexDirection = "column";
            groupWrapper.style.alignItems = "flex-start";
            groupWrapper.style.marginRight = "15px";
            const headerContainer = document.createElement('div');
            headerContainer.style.display = "flex";
            headerContainer.style.alignItems = "center";
            headerContainer.style.gap = "5px";
            const groupCheckbox = document.createElement('input');
            groupCheckbox.type = "checkbox";
            if (!(category in legendGroupStates)) legendGroupStates[category] = true;
            groupCheckbox.checked = legendGroupStates[category];
            const headerLabel = document.createElement('span');
            headerLabel.textContent = category;
            headerLabel.style.fontWeight = 'bold';
            headerContainer.appendChild(groupCheckbox);
            headerContainer.appendChild(headerLabel);
            groupWrapper.appendChild(headerContainer);
            const groupDiv = document.createElement('div');
            groupDiv.style.display = "flex";
            groupDiv.style.flexDirection = "column";
            groupDiv.style.gap = "8px";
            const groupLegendItems = [];
            groups[category].forEach(item => {
                const {ds, index} = item;
                const legendItem = document.createElement('div');
                legendItem.className = 'legend-item';
                legendItem.style.opacity = chart.data.datasets[index].hidden ? 0.5 : 1;
                let markerHTML = `<span style="display:inline-block; width:12px; height:12px; background-color:${ds.borderColor}; margin-right:5px;"></span>`;
                legendItem.innerHTML = markerHTML + ds.label;
                legendItem.style.cursor = 'pointer';
                legendItem.onclick = function () {
                    chart.data.datasets[index].hidden = !chart.data.datasets[index].hidden;
                    legendItem.style.opacity = chart.data.datasets[index].hidden ? 0.5 : 1;
                    chart.update();
                };
                groupDiv.appendChild(legendItem);
                groupLegendItems.push({legendItem, index});
            });
            groupWrapper.appendChild(groupDiv);
            legendContainer.appendChild(groupWrapper);
            groupCheckbox.addEventListener('change', function () {
                let newHidden = !groupCheckbox.checked;
                groupLegendItems.forEach(item => {
                    let idx = item.index;
                    chart.data.datasets[idx].hidden = newHidden;
                    item.legendItem.style.opacity = newHidden ? 0.5 : 1;
                });
                let legendGroupStates = getLegendGroupStates();
                legendGroupStates[category] = groupCheckbox.checked;
                saveLegendGroupStates(legendGroupStates);
                chart.update();
            });
            if (!groupCheckbox.checked) {
                groupLegendItems.forEach(item => {
                    let idx = item.index;
                    chart.data.datasets[idx].hidden = true;
                    item.legendItem.style.opacity = 0.5;
                });
            }
        }
    });
    saveLegendGroupStates(legendGroupStates);
}

/* ================= Data Query and Plotting ================= */
function loadAndPlotData() {
    if (!db) return;
    const yearCheckboxes = document.querySelectorAll("input[name='year']:checked");
    selectedYears = Array.from(yearCheckboxes).map(cb => cb.value);
    const monthCheckboxes = document.querySelectorAll("input[name='month']:checked");
    const selectedMonths = Array.from(monthCheckboxes).map(cb => parseInt(cb.value));
    if (selectedYears.length === 0 || selectedMonths.length === 0) return;
    let storedMapping = localStorage.getItem("yearColorMapping");
    if (storedMapping) {
        yearColorMapping = JSON.parse(storedMapping);
    } else {
        yearColorMapping = {};
    }
    let commonLabelsSet = new Set();
    let yearlyData = {};
    selectedYears.forEach(year => {
        const rows = queryDataByYearAndMonths(year, selectedMonths);
        const data = processYearlyData(rows);
        yearlyData[year] = data;
        Object.keys(data).forEach(day_key => commonLabelsSet.add(day_key));
    });
    const commonLabels = Array.from(commonLabelsSet).sort();
    let datasets = [];
    const currentYearStr = String(new Date().getFullYear());
    selectedYears.forEach(year => {
        const dataForYear = yearlyData[year];
        const minTempData = alignData(commonLabels, dataForYear, "minTempF");
        const maxTempData = alignData(commonLabels, dataForYear, "maxTempF");
        const avgTempData = alignData(commonLabels, dataForYear, "avgTempF");
        const trendData = computeTrendLine(avgTempData);
        const gddData = alignData(commonLabels, dataForYear, "maxCumGDD");
        const rainData = alignData(commonLabels, dataForYear, "rainTotal");
        if (!yearColorMapping[year]) {
            yearColorMapping[year] = randomColor();
        }
        let baseColor = yearColorMapping[year];
        localStorage.setItem("yearColorMapping", JSON.stringify(yearColorMapping));
        let bandFillColor = baseColor.replace("rgb(", "rgba(").replace(")", ",0.3)");
        let colorAvg = lightenColor(baseColor, 0.3);
        let colorRain = lightenColor(baseColor, 0.5);
        let indexMin = datasets.length;
        datasets.push({
            label: `Min Temp (°F) - ${year}`,
            data: minTempData,
            borderColor: 'transparent',
            backgroundColor: bandFillColor,
            borderWidth: 0,
            yAxisID: "y",
            type: 'line',
            fill: false,
            tension: 0.4,
            pointRadius: 0,
            order: 1,
            hideInLegend: true,
            spanGaps: false
        });
        let indexMax = datasets.length;
        datasets.push({
            label: `Min/Max Temp - ${year}`,
            data: maxTempData,
            borderColor: 'transparent',
            backgroundColor: bandFillColor,
            borderWidth: 0,
            yAxisID: "y",
            type: 'line',
            fill: '-1',
            tension: 0.4,
            pointRadius: 0,
            order: 2,
            pairIndices: [indexMin, indexMax],
            spanGaps: false
        });
        datasets.push({
            label: `Avg Temp Trend - ${year}`,
            data: trendData,
            borderColor: darkenColor(colorAvg, 0.8),
            backgroundColor: 'transparent',
            borderDash: [5, 5],
            borderWidth: (year === currentYearStr ? 4 : 2),
            yAxisID: "y",
            type: 'line',
            fill: false,
            tension: 0,
            pointRadius: 0,
            order: 3,
            spanGaps: false
        });
        datasets.push({
            label: `Avg Temp (°F) - ${year}`,
            data: avgTempData,
            borderColor: colorAvg,
            backgroundColor: 'transparent',
            borderWidth: (year === currentYearStr ? 4 : 2),
            yAxisID: "y",
            type: 'line',
            fill: false,
            tension: 0.4,
            pointRadius: 3,
            order: 4,
            spanGaps: false
        });
        datasets.push({
            label: "Cumulative GDD - " + year,
            data: gddData,
            borderColor: baseColor,
            backgroundColor: 'transparent',
            yAxisID: "y1",
            tension: 0.4,
            pointRadius: 2,
            borderDash: [10, 5],
            borderWidth: (year === currentYearStr ? 3 : 2),
            fill: false,
            type: 'line',
            order: 5,
            spanGaps: false
        });
        datasets.push({
            label: "Daily Rain Total (in) - " + year,
            data: rainData,
            backgroundColor: colorRain,
            yAxisID: "y2",
            type: 'bar',
            order: 6,
            spanGaps: false
        });
    });
    // Add Sunspots series
    selectedYears.forEach(year => {
        const sunspotData = querySunspotsDataByYearAndMonths(year, selectedMonths);
        const sunspotsAligned = commonLabels.map(label => sunspotData[label] !== undefined ? sunspotData[label] : null);
        let baseColor = yearColorMapping[year] || randomColor();
        let colorSunspot = lightenColor(baseColor, 0.8);
        datasets.push({
            label: "Sunspots Daily Total - " + year,
            data: sunspotsAligned,
            borderColor: colorSunspot,
            backgroundColor: 'transparent',
            yAxisID: "y3",
            type: "line",
            tension: 0.4,
            pointRadius: 2,
            borderWidth: (year === currentYearStr ? 4 : 2),
            order: 7,
            spanGaps: false
        });
    });
    // Add Pest Spray Points
    const pestDatasets = computePestSprayPoints(selectedYears, commonLabels, yearlyData);
    datasets = datasets.concat(pestDatasets);
    // Add Bud Break datasets
    const budBreakDatasets = computeBudBreakPoints(selectedYears, commonLabels, yearlyData);
    datasets = datasets.concat(budBreakDatasets);
    const legendGroupStates = getLegendGroupStates();
    datasets.forEach(ds => {
        const category = getLegendCategory(ds.label);
        if (category !== "Grape Varietal" && legendGroupStates.hasOwnProperty(category) && !legendGroupStates[category]) {
            ds.hidden = true;
        }
    });
    // Add vertical lines for prediction models (if current year is selected)
    let verticalLines = [];
    const currentYear = String(new Date().getFullYear());
    if (selectedYears.includes(currentYear)) {
        const selectedModel = (document.getElementById("modelSelect") || {}).value || "linearDOYRegression";
        let columnName;
        let dashPattern;
        let labelSuffix;
        switch (selectedModel) {
            case "linearDOYRegression":
                columnName = "regression_projected_bud_break";
                dashPattern = [5, 5];
                labelSuffix = " (Linear DOY Regression)";
                break;
            case "medianGDDEnsemble":
                columnName = "hybrid_projected_bud_break";
                dashPattern = [];
                labelSuffix = " (Median GDD Ensemble)";
                break;
            case "mlMultivariateGDD":
                columnName = "ehml_projected_bud_break";
                dashPattern = [10, 3];
                labelSuffix = " (ML Multivariate GDD)";
                break;
            default:
                columnName = "regression_projected_bud_break";
                dashPattern = [5, 5];
                labelSuffix = " (Linear DOY Regression)";
        }
        const res = db.exec(`SELECT variety, ${columnName}
                             FROM grapevine_gdd`);
        if (res.length > 0) {
            res[0].values.forEach(row => {
                const variety = row[0];
                const predicted = row[1];
                const varietyColor = getColorForVariety(variety);
                if (predicted && predicted.startsWith(currentYear)) {
                    const xLabel = predicted.substring(5);
                    verticalLines.push({
                        xLabel: xLabel,
                        color: varietyColor,
                        label: variety + " Bud Break" + labelSuffix,
                        lineWidth: 2,
                        dashPattern: dashPattern
                    });
                }
            });
        }
    }

    // Add vertical line for today's date
    const today = new Date();
    const month = (today.getMonth() + 1).toString().padStart(2, '0');
    const day = today.getDate().toString().padStart(2, '0');
    const todayStr = month + '-' + day;
    if (commonLabels.includes(todayStr)) {
        verticalLines.push({
            xLabel: todayStr,
            color: 'black',
            label: 'Today',
            lineWidth: 2,
            dashPattern: [5, 5]
        });
    }

    if (dailyChart) dailyChart.destroy();
    const ctx = document.getElementById('dailyChart').getContext('2d');
    dailyChart = new Chart(ctx, {
        type: 'line',
        data: {labels: commonLabels, datasets: datasets},
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: "Daily Avg Temp (with Min/Max Band, Trend & Actual), Cumulative GDD, Daily Rain, Bud Break"
                },
                legend: {display: false},
                verticalLinePlugin: {lines: verticalLines},
                zoom: {
                    zoom: {
                        wheel: {enabled: true, speed: 0.1},
                        pinch: {enabled: true},
                        drag: {enabled: false},
                        mode: 'x'
                    },
                    pan: {enabled: true, mode: 'x'}
                },
                datalabels: {display: false},
                tooltip: {
                    mode: 'nearest',
                    intersect: true,
                    callbacks: {
                        label: function (context) {
                            if (context.dataset.hideInLegend) {
                                const point = context.raw;
                                return context.dataset.label + ": " +
                                    "Scientific: " + point.scientific_name +
                                    ", Stage: " + point.stage +
                                    ", GDD: " + point.y + " (min), " + point.max_gdd + " (max)" +
                                    ", Dormant: " + point.dormant;
                            }
                            return context.dataset.label + ": " + context.formattedValue;
                        }
                    }
                }
            },
            scales: {
                x: {title: {display: true, text: "Month-Day"}},
                y: {position: "left", title: {display: true, text: "Temperature (°F)"}},
                y1: {position: "right", title: {display: true, text: "Cumulative GDD"}, grid: {drawOnChartArea: false}},
                y2: {
                    position: "right",
                    title: {display: true, text: "Rain Total (in)"},
                    grid: {drawOnChartArea: false},
                    beginAtZero: true
                },
                y3: {
                    position: "right",
                    title: {display: true, text: "Sunspots Daily Total"},
                    grid: {drawOnChartArea: false},
                    beginAtZero: true
                }
            }
        }
    });
    generateCustomLegend(dailyChart);
}

function applyGDDGreaterMode() {
    if (!db) return;
    const currentYear = String(new Date().getFullYear());
    const currentYearQuery = `SELECT date, gdd
                              FROM readings
                              WHERE substr(date, 1, 4) = ?
                              ORDER BY date DESC
                              LIMIT 1;`;
    const stmt = db.prepare(currentYearQuery);
    stmt.bind([currentYear]);
    let currentRecord = null;
    if (stmt.step()) {
        currentRecord = stmt.getAsObject();
    }
    stmt.free();
    if (!currentRecord) {
        console.log("No data for current year");
        return;
    }
    const currentGDD = parseFloat(currentRecord.gdd);
    const dayKey = currentRecord.date.substring(5, 10);
    const query = `SELECT substr(date, 1, 4) as year, MAX(gdd) as gdd
                   FROM readings
                   WHERE substr(date, 6, 5) = ?
                   GROUP BY year
                   ORDER BY year ASC;`;
    const stmt2 = db.prepare(query);
    stmt2.bind([dayKey]);
    let qualifyingYears = [currentYear];
    while (stmt2.step()) {
        const row = stmt2.getAsObject();
        const year = row.year;
        if (year === currentYear) continue;
        const yearGDD = parseFloat(row.gdd);
        if (yearGDD > currentGDD) {
            qualifyingYears.push(year);
        }
    }
    stmt2.free();
    const yearCheckboxes = document.querySelectorAll("input[name='year']");
    yearCheckboxes.forEach(cb => {
        cb.checked = qualifyingYears.includes(cb.value);
    });
    selectedYears = qualifyingYears;
    loadAndPlotData();
}

function applyGDDLessMode() {
    if (!db) return;
    const currentYear = String(new Date().getFullYear());
    const currentYearQuery = `SELECT date, gdd
                              FROM readings
                              WHERE substr(date, 1, 4) = ?
                              ORDER BY date DESC
                              LIMIT 1;`;
    const stmt = db.prepare(currentYearQuery);
    stmt.bind([currentYear]);
    let currentRecord = null;
    if (stmt.step()) {
        currentRecord = stmt.getAsObject();
    }
    stmt.free();
    if (!currentRecord) {
        console.log("No data for current year");
        return;
    }
    const currentGDD = parseFloat(currentRecord.gdd);
    const dayKey = currentRecord.date.substring(5, 10);
    const query = `SELECT substr(date, 1, 4) as year, MAX(gdd) as gdd
                   FROM readings
                   WHERE substr(date, 6, 5) = ?
                   GROUP BY year
                   ORDER BY year ASC;`;
    const stmt2 = db.prepare(query);
    stmt2.bind([dayKey]);
    let qualifyingYears = [currentYear];
    while (stmt2.step()) {
        const row = stmt2.getAsObject();
        const year = row.year;
        if (year === currentYear) continue;
        const yearGDD = parseFloat(row.gdd);
        if (yearGDD < currentGDD) {
            qualifyingYears.push(year);
        }
    }
    stmt2.free();
    const yearCheckboxes = document.querySelectorAll("input[name='year']");
    yearCheckboxes.forEach(cb => {
        cb.checked = qualifyingYears.includes(cb.value);
    });
    selectedYears = qualifyingYears;
    loadAndPlotData();
}

function applyNearestNeighborMode() {
    if (!db) return;
    const thresholdInput = document.getElementById("nnThreshold");
    const threshold = parseFloat(thresholdInput ? thresholdInput.value : "10.0") || 10.0;
    const currentYear = String(new Date().getFullYear());
    const currentYearQuery = `SELECT date, gdd
                              FROM readings
                              WHERE substr(date, 1, 4) = ?
                              ORDER BY date DESC
                              LIMIT 1;`;
    const stmt = db.prepare(currentYearQuery);
    stmt.bind([currentYear]);
    let currentRecord = null;
    if (stmt.step()) {
        currentRecord = stmt.getAsObject();
    }
    stmt.free();
    if (!currentRecord) {
        console.log("No data for current year");
        return;
    }
    const currentGDD = parseFloat(currentRecord.gdd);
    const dayKey = currentRecord.date.substring(5, 10);
    const nnQuery = `SELECT substr(date, 1, 4) as year, MAX(gdd) as gdd
                     FROM readings
                     WHERE substr(date, 6, 5) = ?
                     GROUP BY year
                     ORDER BY year ASC;`;
    const stmt2 = db.prepare(nnQuery);
    stmt2.bind([dayKey]);
    let qualifyingYears = [];
    while (stmt2.step()) {
        const row = stmt2.getAsObject();
        const year = row.year;
        const yearGDD = parseFloat(row.gdd);
        if (Math.abs(yearGDD - currentGDD) <= threshold) {
            qualifyingYears.push(year);
        }
    }
    stmt2.free();
    const yearCheckboxes = document.querySelectorAll("input[name='year']");
    yearCheckboxes.forEach(cb => {
        cb.checked = qualifyingYears.includes(cb.value);
    });
    selectedYears = qualifyingYears;
    loadAndPlotData();
}

const modeSelectElement = document.getElementById("modeSelect");
if (modeSelectElement) {
    modeSelectElement.addEventListener("change", function () {
        const mode = this.value;
        if (mode === 'normal') {
            document.getElementById("thresholdContainer").style.display = "none";
            document.querySelectorAll("input[name='year']").forEach(cb => {
                cb.disabled = false;
            });
            const allYearsCb = document.getElementById("allYears");
            if (allYearsCb) allYearsCb.disabled = false;
            loadAndPlotData();
        } else {
            document.getElementById("thresholdContainer").style.display = "none";
            document.querySelectorAll("input[name='year']").forEach(cb => {
                cb.disabled = true;
            });
            const allYearsCb = document.getElementById("allYears");
            if (allYearsCb) allYearsCb.disabled = true;
            if (mode === 'nearest') {
                document.getElementById("thresholdContainer").style.display = "inline";
                applyNearestNeighborMode();
            } else if (mode === 'greater') {
                applyGDDGreaterMode();
            } else if (mode === 'less') {
                applyGDDLessMode();
            }
        }
    });
}

const nnThresholdElement = document.getElementById("nnThreshold");
if (nnThresholdElement) {
    nnThresholdElement.addEventListener("change", function () {
        if ((document.getElementById("modeSelect") || {}).value === "nearest") {
            applyNearestNeighborMode();
        }
    });
}

const tooltipEl = document.getElementById("vlineTooltip");
const dailyChartElement = document.getElementById("dailyChart");
if (dailyChartElement) {
    dailyChartElement.addEventListener("mousemove", function (event) {
        if (!dailyChart) return;
        const canvasPosition = this.getBoundingClientRect();
        const xPos = event.clientX - canvasPosition.left;
        const xScale = dailyChart.scales.x;
        const tolerance = 5;
        let labels = [];
        if (dailyChart.options.plugins.verticalLinePlugin && dailyChart.options.plugins.verticalLinePlugin.lines) {
            dailyChart.options.plugins.verticalLinePlugin.lines.forEach(line => {
                const lineX = xScale.getPixelForValue(line.xLabel);
                if (Math.abs(xPos - lineX) <= tolerance) {
                    labels.push(line.label);
                }
            });
        }
        if (tooltipEl) {
            if (labels.length > 0) {
                tooltipEl.style.display = "block";
                tooltipEl.textContent = labels.join("\n");
                tooltipEl.style.left = (xPos + canvasPosition.left + 10) + "px";
                tooltipEl.style.top = (event.clientY + 10) + "px";
            } else {
                tooltipEl.style.display = "none";
            }
        }
    });
}

const exportButton = document.getElementById("exportButton");
if (exportButton) {
    exportButton.addEventListener("click", function () {
        if (!dailyChart) {
            alert("Chart is not loaded yet.");
            return;
        }
        const link = document.createElement("a");
        link.href = dailyChart.toBase64Image();
        link.download = "chart.png";
        link.click();
    });
}

const toggleLeftPanel = document.getElementById("toggleLeftPanel");
if (toggleLeftPanel) {
    toggleLeftPanel.addEventListener("click", function () {
        const leftPanel = document.getElementById("leftPanel");
        if (leftPanel) {
            leftPanel.classList.toggle("hidden");
            setTimeout(() => window.dispatchEvent(new Event('resize')), 350);
        }
    });
}

const toggleRightPanel = document.getElementById("toggleRightPanel");
if (toggleRightPanel) {
    toggleRightPanel.addEventListener("click", function () {
        const rightPanel = document.getElementById("rightPanel");
        if (rightPanel) {
            rightPanel.classList.toggle("hidden");
            setTimeout(() => window.dispatchEvent(new Event('resize')), 350);
        }
    });
}

window.addEventListener("resize", function () {
    if (dailyChart) dailyChart.resize();
});

window.onload = refreshData;
