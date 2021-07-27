let task;
let nameCounter;
let variables;

let apiCache = {};
let editor = false;

const REP_VAR = /{([A-Z][A-Z0-9_]+?|[A-Z][A-Z0-9_]+?:[^}]*?)}/g;
const REP_INTEGER = /^[-+]?\d+$/;
const REP_DOUBLE = /^[-+]?(?:\d*\.?\d+|\d+\.?\d*)(?:[eE][-+]?\d+)?$/;

const $id = id => document.getElementById(id);


function getTaskTemplate() {
    return {
        items: [],
        streams: {
            '_default': {
                input: {
                    partCount: '1',
                    delimiter: ''
                },
                output: {
                    partCount: '1',
                    delimiter: ''
                }
            }
        },
        input: [],
        output: [],
        prefix: 'spark.meta',
        variables: {}
    };
}

function getOpTemplate(opName, verb) {
    return {
        name: opName,
        verb: verb,
        definitions: {},
        input: {
            positional: '',
            named: {}
        },
        output: {
            positional: '',
            named: {}
        }
    };
}

function getDirTemplate(directive) {
    return {
        directive: directive
    };
}

function getDSTemplate() {
    return {
        input: {
            columns: '',
            delimiter: '',
            partCount: ''
        },
        output: {
            columns: '',
            delimiter: '',
            partCount: ''
        }
    };
}


function getNextName(name) {
    let ret = `${name}_${nameCounter}`;
    nameCounter++;
    return ret;
}

function getNextId() {
    let arr = new Uint8Array(6);
    crypto.getRandomValues(arr);
    return 'id-' + Array.from(arr, byte => ('0' + (byte & 0xFF).toString(16)).slice(-2)).join('');
}

function getDSColor(dsName) {
    let arr = new TextEncoder().encode(dsName);
    let buffer = new Uint8Array(3);
    buffer.fill(0xC0);
    for (let i = 0, j; i < arr.length; i++) {
        j = i % 3;
        buffer[j] ^= arr[i];
    }
    let color = Array.from(buffer).map(i => i.toString(16)).join('');
    return `#${color}`;
}


async function getApi(apiUrl) {
    if (apiCache.hasOwnProperty(apiUrl)) {
        return apiCache[apiUrl];
    }

    let resp = await fetch(apiUrl);
    let obj = await resp.json();
    apiCache[apiUrl] = obj;

    return obj;
}


async function switchLayers(layerId) {
    if (($id(`list-${layerId}`).innerHTML === '') || (layerId === 'foreign')) {
        await initLayer(layerId);
    }

    let switches = document.getElementsByClassName('switch');
    for (let idx = 0; idx < switches.length; idx++) {
        let switchEl = switches[idx];
        if (switchEl.id === `switch-${layerId}`) {
            switchEl.classList.add('switch-active');
        } else {
            switchEl.classList.remove('switch-active');
        }
    }

    let layers = document.getElementsByClassName('layer');
    for (let idx = 0; idx < layers.length; idx++) {
        let layerEl = layers[idx];
        layerEl.style.display = (layerEl.id === `layer-${layerId}`) ? 'block' : 'none';
    }
}

async function initLayer(layerId) {
    switch (layerId) {
        case 'operations' : {
            let opsList = '';

            let packages = await getApi('/package/operations');
            Object.entries(packages).forEach(([name, descr]) => {
                opsList += `<li><span onclick="showOpPackage('${name}')"><span class="i">i<small>${descr}</small></span>${name}</span><ul class="package-list" id="package-${name}"></ul>`;
            });

            $id('list-operations').innerHTML = opsList;

            break;
        }
        case 'foreign' : {
            let layersList = '';
            let layers = {};
            Object.keys(task)
                .filter(key => key.includes('.') && !key.startsWith('input.') && !key.startsWith('output.'))
                .forEach(key => {
                    let kk = key.split('.', 2)[0];
                    layers[kk] = layers[kk] ? layers[kk] + 1 : 1;
                });

            Object.entries(layers).forEach(([layer, keys]) => {
                layersList += `<li><label><button onclick="editLayer('${layer}')">${layer}...</button><small>${keys} key(s)</small></label></li>`;
            });

            $id('list-foreign').innerHTML = layersList;
            break;
        }
        default: {
            let list = '';

            let packages = await getApi(`/package/${layerId}s`);
            Object.entries(packages).forEach(([name, descr]) => {
                list += `<li><span onclick="showAdapterPackage('${name}', '${layerId}')"><span class="i">i<small>${descr}</small></span>${name}</span><ul class="package-list" id="package-${layerId}-${name}"></ul>`;
            });

            $id(`list-${layerId}`).innerHTML = list;
        }
    }
}

function addNewLayer() {
    let newLayerEl = $id('new-layer');
    let layer = newLayerEl.value.trim();
    if (layer.endsWith('.')) {
        layer = layer.substring(0, layer.length - 1);
    }

    if (layer) {
        editLayer(layer);

        newLayerEl.value = '';
    } else {
        newLayerEl.focus();
    }
}

function editLayer(layer) {
    showDialog(`This is current task's foreign configuration layer ${layer}. You may manually edit it as an .ini file fragment`,
        Object.entries(task)
            .filter(([key, _]) => key.startsWith(`${layer}.`))
            .map(([key, value]) => `${key}=${value}`)
            .join('\n'), 'Accept changes', parseLayer, layer);
}

async function parseLayer(text, [layer]) {
    cancelDialog();

    Object.keys(task)
        .filter(key => key.startsWith(`${layer}.`))
        .forEach(key => delete task[key]);

    text.split(/[\r\n]+/).forEach(p => {
        let pair = p.split('=', 2);
        if (pair.length === 2) {
            let key = pair[0].trim();
            if (key) {
                if (!key.startsWith(`${layer}.`)) {
                    key = `${layer}.${key}`;
                }
                task[key] = pair[1].trim();
            }
        }
    });

    await switchLayers('foreign');
}


function initDefaultDS() {
    let canvas = $id('canvas-ds');
    canvas.innerHTML = '';

    let defaultInputEl = document.createElement('DIV');
    defaultInputEl.className = 'ds input';
    defaultInputEl.id = 'default-input';
    defaultInputEl.innerHTML = `<span id="default-input-btn-extra" class="btn-extra"></span><button id="default-input-btn-props" onclick="editDefaultDS('input')">Edit</button> <b>Default</b> <small>input properties</small>`;

    canvas.insertAdjacentElement('afterbegin', defaultInputEl);

    let defaultOutputEl = document.createElement('DIV');
    defaultOutputEl.className = 'ds output';
    defaultOutputEl.id = 'default-output';
    defaultOutputEl.innerHTML = `<span id="default-output-btn-extra" class="btn-extra"></span><button id="default-output-btn-props" onclick="editDefaultDS('output')">Edit</button> <b>Default</b> <small>output properties</small>`;

    canvas.insertAdjacentElement('beforeend', defaultOutputEl);
}

function editDefaultDS(kind) {
    let defaultEl = $id(`default-${kind}`);
    $id(`default-${kind}-btn-props`).disabled = true;
    $id(`default-${kind}-btn-extra`).innerHTML = `<button onclick="applyDefaultDS('${kind}');cancelDefaultDS('${kind}')">Apply</button><button onclick="cancelDefaultDS('${kind}')">Cancel</button>`;

    let defaultEditEl = document.createElement('DL');
    defaultEditEl.className = 'props';
    defaultEditEl.id = `default-${kind}-props`;

    let dsKind = task.streams._default[kind];

    let pathId = `default-${kind}-path`;
    let pathDef = {type: 'String'};
    let pathSel = getSelectDef(pathId, pathDef, task[`${kind}.path`]);
    let partsId = `default-${kind}-parts`;
    let partsDef = {type: 'Integer'};
    let partsSel = getSelectDef(partsId, partsDef, dsKind.partCount);
    let delId = `default-${kind}-delimiter`;
    let delDef = {type: 'String'};
    let delSel = getSelectDef(delId, delDef, dsKind.delimiter);

    defaultEditEl.innerHTML = `<div id="${pathId}" class="tr-def"><dt><label for="inp-${pathId}">Path</label></dt><dd>${pathSel}</dd></div>
<div id="${partsId}" class="tr-def"><dt><label for="inp-${partsId}">Part count</label></dt><dd>${partsSel}</dd></div>
<div id="${delId}" class="tr-def"><dt><label for="inp-${delId}">Delimiter</label></dt><dd>${delSel}</dd></div>`;
    defaultEl.appendChild(defaultEditEl);
}

function applyDefaultDS(kind) {
    let dsKind = task.streams._default[kind];

    dsKind.delimiter = $id(`inp-default-${kind}-delimiter`).value.trim();
    dsKind.partCount = $id(`inp-default-${kind}-parts`).value.trim();
    task[`${kind}.path`] = $id(`inp-default-${kind}-path`).value.trim();

    updateVariables(dsKind);
}

function cancelDefaultDS(kind) {
    let defaultEl = $id(`default-${kind}`);
    defaultEl.removeChild($id(`default-${kind}-props`));
    $id(`default-${kind}-btn-extra`).innerHTML = '';
    $id(`default-${kind}-btn-props`).disabled = false;
}


async function showOpPackage(name) {
    let pkgList = $id(`package-${name}`);
    if (getComputedStyle(pkgList).display !== 'block') {
        if (pkgList.innerHTML === '') {
            let pkgOps = await getApi(`/package/operations/${name}`);

            let pkgHtml = '';
            Object.entries(pkgOps).forEach(([name, descr]) => {
                pkgHtml += `<li><label><button onclick="addNewOp('${name}')">${name}</button><small>${descr}</small></label></li>`;
            });

            pkgList.innerHTML = pkgHtml;
        }

        pkgList.style.display = 'block';
    } else {
        pkgList.style.display = 'none';
    }
}

async function showAdapterPackage(name, kind) {
    let pkgList = $id(`package-${kind}-${name}`);
    if (getComputedStyle(pkgList).display !== 'block') {
        if (pkgList.innerHTML === '') {
            let pkgOps = await getApi(`/package/${kind}s/${name}`);

            let pkgHtml = '';
            Object.entries(pkgOps).forEach(([name, descr]) => {
                pkgHtml += `<li><label><button onclick="addNewAdapter('${name}', '${kind}')">${name}</button><small>${descr}</small></label></li>`;
            });

            pkgList.innerHTML = pkgHtml;
        }

        pkgList.style.display = 'block';
    } else {
        pkgList.style.display = 'none';
    }
}


async function addAdapter(adapterName, kind, name) {
    let adapterDef = await getApi(`/adapter/${kind}/${adapterName}`);

    let adapterEl = document.createElement('DIV');
    let adapterId = getNextId();
    adapterEl.id = adapterId;
    adapterEl.innerHTML = `<span class="btn-right">${kind} ${adapterName}<span class="i">i<small>${adapterDef.descr}</small></span><button class="btn-remove" onclick="removeAdapter('${adapterId}', '${kind}')" title="Remove">×</button></span>
<span id="name-${adapterId}" class="adapter-name">${name}</span>
<input id="inp-${adapterId}" value="${name}" type="hidden"><button id="btn-edit-${adapterId}" class="btn-edit" onclick="editAdapter('${adapterId}', '${kind}')">Edit</button>
<span id="btn-extra-${adapterId}" class="btn-extra"></span>
<input type="hidden" id="verb-${adapterId}" value="${adapterName}"><input type="hidden" id="ref-${adapterId}" value="${name}">`;
    adapterEl.className = `adapter ${kind}`;
    adapterEl.style.backgroundColor = getDSColor(name);

    let canvas = $id("canvas-task");

    let adapters = canvas.getElementsByClassName(`adapter ${kind}`);
    if (adapters.length) {
        adapters[adapters.length - 1].after(adapterEl);
    } else {
        let position;
        switch (kind) {
            case 'input': {
                position = 'afterbegin';
                break;
            }
            case 'output': {
                position = 'beforeend';
                break;
            }
        }
        canvas.insertAdjacentElement(position, adapterEl);
    }

    return adapterId;
}

async function addNewAdapter(adapterName, kind) {
    let name = getNextName(kind);

    let adapterDef = await getApi(`/adapter/${kind}/${adapterName}`);

    switch (kind) {
        case 'input': {
            task.input.unshift(name);
            break;
        }
        case 'output' : {
            task.output.push(name);
            break;
        }
    }
    task.streams[name] = getDSTemplate();
    task[`${kind}.path.${name}`] = adapterDef.pattern;

    let adapterId = await addAdapter(adapterName, kind, name);
    flashNew(adapterId);
    if (!editor) {
        await editAdapter(adapterId, kind);
    } else {
        await describeAdapter(adapterId, kind);
    }
}

async function describeAdapter(adapterId, kind) {
    let adapterEl = $id(adapterId);

    let verb = $id(`verb-${adapterId}`).value;
    let adapterDef = await getApi(`/adapter/${kind}/${verb}`);

    let descrEl = document.createElement('PRE');
    descrEl.className = 'descr';
    descrEl.id = `descr-${adapterId}`;

    let dsName = $id(`ref-${adapterId}`).value;
    let found = task[`${kind}.path.${dsName}`];
    let adapterDescr = `${kind}.path.${dsName}=${found ? found : ''}\n`;

    if (adapterDef.settings) {
        adapterDescr += '\n';
        Object.entries(adapterDef.settings).forEach(([name, def]) => {
            let found = task[`${kind}.${name}.${dsName}`];
            adapterDescr += `${kind}.${name}.${dsName}=${found ? found : ''}\n`;
        });
    }

    descrEl.innerText = adapterDescr;
    adapterEl.appendChild(descrEl);
}

async function editAdapter(adapterId, kind) {
    if (editor) {
        flashEditor();
        return;
    }

    let adapterEl = $id(adapterId);
    editor = adapterEl;

    let descrEl = $id(`descr-${adapterId}`);
    if (descrEl) {
        adapterEl.removeChild(descrEl);
    }

    let btnEdit = $id(`btn-edit-${adapterId}`);
    btnEdit.disabled = true;
    let verb = $id(`verb-${adapterId}`).value;

    $id(`btn-extra-${adapterId}`).innerHTML = `<button onclick="applyAdapter('${adapterId}', '${kind}');cancelAdapter('${adapterId}');describeAdapter('${adapterId}', '${kind}')">Apply</button><button onclick="cancelAdapter('${adapterId}');describeAdapter('${adapterId}', '${kind}')">Cancel</button>`;

    let adapterDef = await getApi(`/adapter/${kind}/${verb}`);

    let propsEl = document.createElement('DL');
    propsEl.className = 'props';
    propsEl.id = `props-${adapterId}`;

    let dsId = `ds-${adapterId}`;
    let dsName = $id(`ref-${adapterId}`).value;
    let dsSel = getSelectDS(dsId, {noVariables: true}, dsName);

    let pathId = `path-${adapterId}`;
    let pathDef = {
        type: 'String'
    };
    let pathSel = getSelectDef(pathId, pathDef, task[`${kind}.path.${dsName}`]);
    let propsHtml = `<div id="${dsId}" class="tr-${kind}"><dt><label class="label-sel" for="sel-${dsId}">Data stream</label></dt>
<dd>${dsSel}</dd></div>
<div id="${pathId}" class="tr-${kind}"><dt><label class="label-sel" for="sel-${pathId}">Path</label><label class="label-inp" for="inp-${pathId}"><small>Pattern is ${adapterDef.pattern}</small></label></dt>
<dd>${pathSel}</dd></div>`;

    if (adapterDef.settings) {
        Object.entries(adapterDef.settings).forEach(([name, def]) => {
            let defId = getNextId();
            let found = task[`${kind}.${name}.${dsName}`];
            let defSel = getSelectDef(defId, def, found ? found : '');
            propsHtml += `<div id="${defId}" class="tr-def"><dt><label class="label-sel" for="sel-${defId}">${name}</label><label class="label-inp" for="inp-${defId}"><small>${def.descr}</small></label></dt>
<dd id="${defId}">${defSel}<input id="name-${defId}" type="hidden" value="${name}"></dd></div>`;
        });
    }

    propsEl.innerHTML = propsHtml;
    adapterEl.appendChild(propsEl);
}

function applyAdapter(adapterId, kind) {
    let adapterEl = $id(adapterId);
    let nameEl = $id(`ref-${adapterId}`);
    let oldName = nameEl.value;
    let reName = $id(`inp-ds-${adapterId}`).value.trim();
    nameEl.value = reName;
    $id(`name-${adapterId}`).innerHTML = reName;

    let ds = task.streams[oldName];
    delete task.streams[oldName];

    let idx = task[kind].length ? task[kind].findIndex(name => name === oldName) : -1;
    if (idx >= 0) {
        task[kind].splice(idx, 1);
    }

    task[kind].push(reName);
    task.streams[reName] = ds;

    Object.keys(task)
        .filter(key => key.startsWith(`${kind}.`) && key.endsWith(`.${oldName}`))
        .forEach(key => delete task[key]);

    let adapterDefs = adapterEl.getElementsByClassName("tr-def");
    let settings = {};
    Array.from(adapterDefs).forEach(tr => {
        let defId = tr.id;
        let defName = $id(`name-${defId}`).value;
        let defVal = $id(`inp-${defId}`).value.trim();
        let defDef = $id(`inp-${defId}`).placeholder;

        let key = `${kind}.${defName}.${reName}`;
        if ((defVal === '') || (defVal === defDef)) {
            delete task[key];
        } else {
            settings[key] = task[key] = defVal;
        }
    });

    let pathKey = `${kind}.path.${reName}`;
    settings[pathKey] = task[pathKey] = $id(`inp-path-${adapterId}`).value.trim();

    adapterEl.style.backgroundColor = getDSColor(reName);

    updateVariables(settings);
}

function cancelAdapter(adapterId) {
    editor = false;

    let adapterEl = $id(adapterId);
    let adapterName = $id(`name-${adapterId}`).value;
    let reNameEl = $id(`inp-${adapterId}`);
    reNameEl.value = adapterName;
    reNameEl.readOnly = true;
    adapterEl.removeChild($id(`props-${adapterId}`));
    $id(`btn-edit-${adapterId}`).disabled = false;
    $id(`btn-extra-${adapterId}`).innerHTML = '';
}

function removeAdapter(adapterId, kind) {
    let adapterEl = $id(adapterId);

    if (editor === adapterEl) {
        cancelAdapter(adapterId);
    }

    let adapterName = $id(`name-${adapterId}`).value;
    adapterEl.parentElement.removeChild(adapterEl);

    Object.keys(task)
        .filter(key => key.startsWith(`${kind}.`) && key.endsWith(`.${adapterName}`))
        .forEach(key => delete task[key]);

    let idx = task[kind].length ? task[kind].findIndex(name => name === adapterName) : -1;
    if (idx >= 0) {
        task[kind].splice(idx, 1);
    }

    tryRemoveDS(adapterName);
}


function renumberTaskItems() {
    let taskItems = document.getElementsByClassName('task-item');
    for (let idx = 0; idx < taskItems.length; idx++) {
        let positionEl = taskItems[idx].getElementsByClassName('task-position')[0];
        positionEl.value = idx;
    }
}

function moveUp(elId) {
    let el = $id(elId);
    let prevEl = el.previousElementSibling;
    if (prevEl && !prevEl.classList.contains('adapter')) {
        prevEl.before(el);

        let position = el.getElementsByClassName('task-position')[0].value;
        let ti = task.items.splice(position, 1);
        task.items.splice(position - 1, 0, ti);
    }

    renumberTaskItems();
}

function moveDown(elId) {
    let el = $id(elId);
    let nextEl = el.nextElementSibling;
    if (nextEl && !nextEl.classList.contains('adapter')) {
        nextEl.after(el);

        let position = el.getElementsByClassName('task-position')[0].value;
        let ti = task.items.splice(position, 1);
        task.items.splice(position + 1, 0, ti);
    }

    renumberTaskItems();
}


function flashNew(elId) {
    let el = $id(elId);
    el.classList.add('flash-new');
    el.scrollIntoView();
    setTimeout(removeFlash, 1100, el, 'flash-new');
}

function flashEditor() {
    if (!editor.classList.contains('flash-editor')) {
        editor.classList.add('flash-editor');
        editor.scrollIntoView();
        setTimeout(removeFlash, 1100, editor, 'flash-editor');
    }
}

function removeFlash(el, flash) {
    el.classList.remove(flash);
}


async function addOp(verb, name, idx) {
    let opDef = await getApi(`/operation/${verb}`);

    let opEl = document.createElement('DIV');
    let opId = getNextId();
    opEl.id = opId;

    opEl.innerHTML = `<span class="btn-right">${verb}<span class="i">i<small>${opDef.descr}</small></span><button class="btn-remove" onclick="removeOp('${opId}')" title="Remove">×</button></span>
<button class="btn-move" onclick="moveUp('${opId}')" title="Move up">↑</button><button class="btn-move" onclick="moveDown('${opId}')" title="Move down">↓</button>
<input id="inp-${opId}" class="inp-rename" value="${name}" readonly><button id="btn-edit-${opId}" class="btn-edit" onclick="editOp('${opId}')">Edit</button>
<span id="btn-extra-${opId}" class="btn-extra"></span>
<input type="hidden" id="verb-${opId}" value="${verb}"><input type="hidden" id="name-${opId}" value="${name}"><input id="position-${opId}" type="hidden" class="task-position" value="${idx}">`;
    opEl.className = 'op task-item';

    let canvas = $id("canvas-task");

    let adapters = canvas.getElementsByClassName('adapter output');
    if (adapters.length) {
        adapters[0].before(opEl);
    } else {
        canvas.insertAdjacentElement('beforeend', opEl);
    }

    return opId;
}

async function addNewOp(verb) {
    let name = getNextName(verb);
    let idx = task.items.length;
    task.items.push(getOpTemplate(name, verb));

    let opId = await addOp(verb, name, idx);

    flashNew(opId);
    if (!editor) {
        await editOp(opId);
    } else {
        await describeOp(opId);
    }
}

async function describeOp(opId) {
    let opEl = $id(opId);

    let verb = $id(`verb-${opId}`).value;
    let op = task.items[$id(`position-${opId}`).value];

    let opDef = await getApi(`/operation/${verb}`);

    let descrEl = document.createElement('PRE');
    descrEl.className = 'descr';
    descrEl.id = `descr-${opId}`;

    let opDescr = '';
    if (opDef.input.positional != null) {
        opDescr += `op.inputs.${op.name}=${op.input.positional}\n`;
    } else {
        Object.entries(opDef.input.streams).forEach(([name, ni]) => {
            let found = Object.keys(op.input.named).find(n => n === name);
            opDescr += `op.input.${op.name}.${name}=${found ? op.input.named[found] : ''}\n`;
        });
    }

    opDescr += '\n';

    if (opDef.definitions) {
        Object.entries(opDef.definitions).forEach(([name, def]) => {
            if (def.optional) {
                let found = Object.keys(op.definitions).find(n => n === name);
                opDescr += `op.definition.${op.name}.${name}=${found ? op.definitions[found] : def.defaults}\n`;
            } else if (def.dynamic) {
                let found = Object.keys(op.definitions).filter(n => n.startsWith(name));
                found.forEach(dyn => {
                    let dynDef = op.definitions[dyn];
                    opDescr += `op.definition.${op.name}.${dyn}=${dynDef}\n`;
                });
            } else {
                let found = Object.keys(op.definitions).find(n => n === name);
                opDescr += `op.definition.${op.name}.${name}=${found ? op.definitions[found] : ''}\n`;
            }
        });

        opDescr += '\n';
    }

    if (opDef.output.positional != null) {
        opDescr += `op.outputs.${op.name}=${op.output.positional}\n`;
    } else {
        Object.entries(opDef.output.streams).forEach(([name, no]) => {
            let found = Object.keys(op.output.named).find(n => n === name);
            opDescr += `op.output.${op.name}.${name}=${found ? op.output.named[found] : ''}\n`;
        });
    }

    descrEl.innerText = opDescr;
    opEl.appendChild(descrEl);
}

async function editOp(opId) {
    if (editor) {
        flashEditor();
        return;
    }

    let opEl = $id(opId);
    editor = opEl;

    let descrEl = $id(`descr-${opId}`);
    if (descrEl) {
        opEl.removeChild(descrEl);
    }

    let btnEdit = $id(`btn-edit-${opId}`);
    btnEdit.disabled = true;
    let verb = $id(`verb-${opId}`).value;
    let op = task.items[$id(`position-${opId}`).value];

    let opDef = await getApi(`/operation/${verb}`);

    let propsEl = document.createElement('DL');
    propsEl.className = 'props';
    propsEl.id = `props-${opId}`;

    let buttonsEl = $id(`btn-extra-${opId}`);
    buttonsEl.innerHTML = `<button onclick="applyOp('${opId}');cancelOp('${opId}');describeOp('${opId}')">Apply</button><button onclick="cancelOp('${opId}');describeOp('${opId}')">Cancel</button>`;

    let propsHtml = '';

    if (opDef.input.positional != null) {
        let dsId = getNextId();
        let dsSel = getSelectDS(dsId, {multiple: true}, op.input.positional);
        propsHtml += `<div id="${dsId}" class="tr-input"><dt><span class="btn-right"><button onclick="editDS('${dsId}', '${verb}', 'input')" id="btn-props-${dsId}">Properties</button></span>
<label class="label-sel" for="sel-${dsId}">At least ${opDef.input.positional} positional input(s) of type ${opDef.input.streams.type}</label></dt>
<dd>${dsSel}<input type="hidden" id="name-${dsId}" value=""></dd></div>`;
    } else {
        Object.entries(opDef.input.streams).forEach(([name, ni]) => {
            let dsId = getNextId();
            let found = Object.keys(op.input.named).find(n => n === name);
            let dsSel = getSelectDS(dsId, null, found ? op.input.named[found] : '');
            propsHtml += `<div id="${dsId}" class="tr-input"><dt><span class="btn-right"><button onclick="editDS('${dsId}', '${verb}', 'input')" id="btn-props-${dsId}">Properties</button></span>
<label class="label-sel" for="sel-${dsId}">Input <b>${name}</b> of type ${ni.type}</label><label class="label-inp" for="inp-${dsId}"><small>${ni.descr}</small></label></dt>
<dd>${dsSel}<input type="hidden" id="name-${dsId}" value="${name}"></dd></div>`;
        });
    }

    if (opDef.definitions) {
        Object.entries(opDef.definitions).forEach(([name, def]) => {
            if (def.optional) {
                let defId = getNextId();
                let found = Object.keys(op.definitions).find(n => n === name);
                let defSel = getSelectDef(defId, def, found ? op.definitions[found] : '');
                propsHtml += `<div id="${defId}" class="tr-def"><dt><label class="label-sel" for="sel-${defId}">${name}</label><label class="label-inp" for="inp-${defId}"><small>${def.descr}</small></label></dt>
<dd id="${defId}">${defSel}<input id="name-${defId}" type="hidden" value="${name}"></dd></div>`;
            } else if (def.dynamic) {
                let dynId = getNextId();
                propsHtml += `<div id="${dynId}" class="tr-dyn"><dt><label class="label-sel" for="inp-${dynId}">${name}</label><label class="label-inp" for="inp-${dynId}"><small>${def.descr}</small></label></dt>
<dd><input id="name-${dynId}" type="hidden" value="${name}"><input id="inp-${dynId}" class="inp-rename"><button onclick="addDynDef('${dynId}', '${verb}')">Add</button></dd>`;
                let found = Object.keys(op.definitions).filter(n => n.startsWith(name));
                found.forEach(dyn => {
                    let dynDef = op.definitions[dyn];
                    let defId = getNextId();
                    let defSel = getSelectDef(defId, def, dynDef);
                    let defName = dyn.substring(name.length);
                    propsHtml += `<div id="${defId}" class="tr-def"><dd><label class="label-sel" for="sel-${defId}" class="dyn-name">${defName}</label><button onclick="removeDynDef('${defId}')">Remove</button>
<br>${defSel}<input id="name-${defId}" type="hidden" value="${defName}"></dd></div>`;
                });
                propsHtml += '</div>';
            } else {
                let defId = getNextId();
                let found = Object.keys(op.definitions).find(n => n === name);
                let defSel = getSelectDef(defId, def, found ? op.definitions[found] : '');
                propsHtml += `<div id="${defId}" class="tr-def"><dt><label class="label-sel" for="sel-${defId}">${name}</label><label class="label-inp" for="inp-${defId}"><small>${def.descr}</small></label></dt>
<dd id="${defId}">${defSel}<input id="name-${defId}" type="hidden" value="${name}"></dd></div>`;
            }
        });
    }

    if (opDef.output.positional != null) {
        let dsId = getNextId();
        let dsSel = getSelectDS(dsId, {multiple: true}, op.output.positional);
        propsHtml += `<div id="${dsId}" class="tr-output"><dt><span class="btn-right"><button onclick="editDS('${dsId}', '${verb}', 'output')" id="btn-props-${dsId}">Properties</button></span>
<label class="label-sel" for="sel-${dsId}">Positional output(s) of type ${opDef.output.streams.type}</label></dt>
<dd>${dsSel}<input type="hidden" id="name-${dsId}" value=""></dd></div>`;
    } else {
        Object.entries(opDef.output.streams).forEach(([name, no]) => {
            let dsId = getNextId();
            let found = Object.keys(op.output.named).find(n => n === name);
            let dsSel = getSelectDS(dsId, null, found ? op.output.named[found] : '');
            propsHtml += `<div id="${dsId}" class="tr-output"><dt><span class="btn-right"><button onclick="editDS('${dsId}', '${verb}', 'output')" id="btn-props-${dsId}">Properties</button></span>
<label class="label-sel" for="sel-${dsId}">Output ${name} of type ${no.type}</label><label class="label-inp" for="inp-${dsId}"><small>${no.descr}</small></label></dt>
<dd>${dsSel}<input type="hidden" id="name-${dsId}" value="${name}"></dd></div>`;
        });
    }

    propsEl.innerHTML = propsHtml;
    opEl.appendChild(propsEl);

    let reNameEl = $id(`inp-${opId}`);
    reNameEl.readOnly = false;
    reNameEl.focus();
}

function applyOp(opId) {
    let opEl = $id(opId);
    let nameEl = $id(`name-${opId}`);
    let reName = $id(`inp-${opId}`).value;
    let verb = $id(`verb-${opId}`).value;

    let op = getOpTemplate(reName, verb);
    let idx = opEl.getElementsByClassName('task-position')[0].value;

    let oldOp = task.items.splice(idx, 1, op)[0];
    nameEl.value = reName;

    let dsSaveButtons = $id('canvas-ds').getElementsByClassName('btn-ds-save');
    Array.from(dsSaveButtons).forEach(btn => btn.click());

    let dataStreams = new Set();

    let opIns = opEl.getElementsByClassName("tr-input");
    Array.from(opIns).forEach(tr => {
        let dsId = tr.id;
        let dsRef = $id(`name-${dsId}`).value;
        let dsName = $id(`inp-${dsId}`).value.trim();
        if (dsRef) {
            op.input.named[dsRef] = dsName;
            if (dsName) {
                dataStreams.add(dsName);
            }
        } else {
            let dsNames = dsName.split(',').filter(n => n.trim());
            op.input.positional = dsNames.join(',');
            dsNames.forEach(n => dataStreams.add(n));
        }
    });

    let opOuts = opEl.getElementsByClassName("tr-output");
    Array.from(opOuts).forEach(tr => {
        let dsId = tr.id;
        let dsRef = $id(`name-${dsId}`).value;
        let dsName = $id(`inp-${dsId}`).value.trim();
        if (dsRef) {
            op.output.named[dsRef] = dsName;
            if (dsName) {
                dataStreams.add(dsName);
            }
        } else {
            let dsNames = dsName.split(',').filter(n => n.trim());
            op.output.positional = dsNames.join(',');
            dsNames.forEach(n => dataStreams.add(n));
        }
    });

    tryRemoveDS(oldOp.input.positional);
    tryRemoveDS(oldOp.input.named);
    tryRemoveDS(oldOp.output.positional);
    tryRemoveDS(oldOp.output.named);

    dataStreams.forEach(dsName => {
        if (!task.streams[dsName]) {
            task.streams[dsName] = getDSTemplate();
        }
    });

    let opDefs = opEl.getElementsByClassName("tr-def");
    Array.from(opDefs).forEach(tr => {
        let defId = tr.id;
        let defName = $id(`name-${defId}`).value;
        let defVal = $id(`inp-${defId}`).value.trim();
        let defDef = $id(`inp-${defId}`).placeholder;

        op.definitions[defName] = ((defVal === '') || (defVal === defDef)) ? null : defVal;
    });

    updateVariables(op);
}

function cancelOp(opId) {
    editor = false;

    let dsCancelButtons = $id('canvas-ds').getElementsByClassName('btn-ds-cancel');
    Array.from(dsCancelButtons).forEach(btn => btn.click());

    let opEl = $id(opId);
    let opName = $id(`name-${opId}`).value;
    let reNameEl = $id(`inp-${opId}`);
    reNameEl.value = opName;
    reNameEl.readOnly = true;
    opEl.removeChild($id(`props-${opId}`));
    $id(`btn-edit-${opId}`).disabled = false;
    $id(`btn-extra-${opId}`).innerHTML = '';
}

function removeOp(opId) {
    let opEl = $id(opId);

    if (editor === opEl) {
        cancelOp(opId);
    }

    let idx = opEl.getElementsByClassName("task-position")[0].value;
    let op = task.items.splice(idx, 1)[0];
    tryRemoveDS(op.input.positional);
    tryRemoveDS(op.input.named);
    tryRemoveDS(op.output.positional);
    tryRemoveDS(op.output.named);

    opEl.parentElement.removeChild(opEl);

    renumberTaskItems();
}


function getSelectDS(dsId, def, value = '') {
    let withVars = true;
    let multiple = false;
    if (def) {
        withVars = !def.noVariables;
        multiple = def.multiple;
    }
    let selHtml = `<select id="sel-${dsId}" class="sel-ds" onchange="selectDS('${dsId}', ${multiple})"><optgroup label="Task data streams">`;

    let literal = true;
    Object.keys(task.streams).forEach((name) => {
        if (name !== '_default') {
            if (name === value) {
                selHtml += `<option selected>${name}</option>`;
                literal = false;
            } else {
                selHtml += `<option>${name}</option>`;
            }
        }
    });

    selHtml += `</optgroup><optgroup label="Literal${withVars ? ' or add {VARIABLE}' : ''}">`;
    if (withVars) {
        Object.entries(variables).forEach(([k, v]) => {
            if (value === `{${k}}`) {
                selHtml += `<option value="{${k}}" selected>{${k}} ${v}</option>`;
                literal = false;
            } else {
                selHtml += `<option value="{${k}}">{${k}} ${v}</option>`;
            }
        });
    }

    if (literal) {
        selHtml += `<option value="" selected>Literal</option>`;
    } else {
        selHtml += `<option value="">Literal</option>`;
    }

    selHtml += `</optgroup></select><input id="inp-${dsId}" class="inp-value type-literal" value="${value}">`;
    return selHtml;
}

function selectDS(dsId, multiple = false) {
    let selVal = $id(`sel-${dsId}`).value;
    let dsInp = $id(`inp-${dsId}`);

    switch (selVal) {
        case '' : {
            break;
        }
        default : {
            if (selVal.startsWith('{')) {
                dsInp.value += selVal;
            } else {
                if (multiple && dsInp.value) {
                    dsInp.value = `${dsInp.value},${selVal}`;
                } else {
                    dsInp.value = selVal;
                }
            }
            break;
        }
    }
    dsInp.focus();
}

function getSelectDef(defId, def, value = '') {
    if (value === null) {
        value = '';
    }

    let selHtml = `<select id="sel-${defId}" class="sel-def" onchange="selectDef('${defId}')">`;

    let literal = true;
    let inputProps = 'class="inp-value type-literal" type="text"';
    let withVars = true, onlyVars = false;
    if (def) {
        if (def.defaults) {
            let defaults = def.defaults;
            if (defaults === null) {
                defaults = '';
            }
            if (value === '') {
                literal = false;
            }
            selHtml += `<optgroup label="Set to default value"><option value="${defaults}" ${(defaults === value) ? 'selected' : ''}>${def.defaults} ${def.defDescr}</option></optgroup>`;
            inputProps += ` placeholder="${def.defaults}"`;
        }

        if (def.generated && def.generated.length) {
            selHtml += `<optgroup label="Add generated value">`;
            Object.entries(def.generated).forEach(([name, descr]) => {
                if (value === name) {
                    selHtml += `<option value=",${name}" selected>${name} ${descr}</option>`;
                    literal = false;
                } else {
                    selHtml += `<option value=",${name}">${name} ${descr}</option>`;
                }
            });
            selHtml += '</optgroup>';
        }

        if (def.values && def.values.length) {
            selHtml += `<optgroup label="One of ${def.type ? def.type : 'predefined values'}">`;
            Object.entries(def.values).forEach(([name, descr]) => {
                if (value === name) {
                    selHtml += `<option value="${name}" selected>${name} ${descr}</option>`;
                    literal = false;
                } else {
                    selHtml += `<option value="${name}">${name} ${descr}</option>`;
                }
            });
            selHtml += '</optgroup>';
        }

        if (def.type && !def.type.startsWith('String')) {
            selHtml += `<optgroup label="Of type ${def.type}">`;
            let match = false;

            switch (def.type) {
                case 'Double' : {
                    if (value) {
                        match = value.match(REP_DOUBLE);
                        if (match) {
                            inputProps = 'class="inp-value type-number" type="number" step="any"';
                            literal = false;
                        }
                    }
                    break;
                }
                case 'Byte':
                case 'Integer':
                case 'Long': {
                    if (value) {
                        match = value.match(REP_INTEGER);
                        if (match) {
                            inputProps = 'class="inp-value type-number" type="number" step="1"';
                            literal = false;
                        }
                    }
                    break;
                }
                case 'Boolean' : {
                    if (value) {
                        match = (value === 'true') || (value === 'false');
                        if (match) {
                            inputProps = `class="inp-value type-boolean" type="checkbox" onchange="defCheck" value="${value}"`;
                            if (value === 'true') {
                                inputProps += ' checked';
                            }
                            literal = false;
                        }
                    }
                    break;
                }
            }

            if (match) {
                selHtml += `<option value="${def.type}" selected>Typed</option>`;
            } else {
                selHtml += `<option value="${def.type}">Typed</option>`;
            }
            selHtml += '</optgroup>';
        }

        onlyVars = def.onlyVars;
        withVars = !def.noVariables && !onlyVars;
    }

    if (withVars) {
        selHtml += `<optgroup label="Literal or add {VARIABLE}">`;
        Object.entries(variables).forEach(([k, v]) => {
            if (value === `{${k}}`) {
                selHtml += `<option value="{${k}}" selected>{${k}} ${v}</option>`;
                literal = false;
            } else {
                selHtml += `<option value="{${k}}">{${k}} ${v}</option>`;
            }
        });
    }
    if (onlyVars) {
        selHtml += `<optgroup label="Select or add new {VARIABLE}">`;
        Object.entries(variables).forEach(([k, v]) => {
            if (value === `${k}`) {
                selHtml += `<option value="${k}" selected>{${k}} ${v}</option>`;
                literal = false;
            } else {
                selHtml += `<option value="${k}">{${k}} ${v}</option>`;
            }
        });
    }

    if (literal) {
        selHtml += `<option value="" selected>Literal</option>`;
    } else {
        selHtml += `<option value="">Literal</option>`;
    }

    selHtml += `</optgroup></select><input id="inp-${defId}" ${inputProps} value="${value}">`;
    return selHtml;
}

function defCheck(ev) {
    if (ev.target.type === 'checkbox') {
        ev.target.value = ev.target.checked + '';
    }
    ev.stopPropagation();
}

function selectDef(defId) {
    let selVal = $id(`sel-${defId}`).value;
    let defInp = $id(`inp-${defId}`);

    defInp.readOnly = false;
    switch (selVal) {
        case 'Double': {
            defInp.type = 'number';
            defInp.className = 'inp-value type-number';
            defInp.step = 'any';
            break;
        }
        case 'Byte':
        case 'Integer':
        case 'Long': {
            defInp.type = 'number';
            defInp.className = 'inp-value type-number';
            defInp.step = 1;
            break;
        }
        case 'Boolean' : {
            defInp.type = 'checkbox';
            defInp.className = 'inp-value type-boolean';
            defInp.value = 'true';
            defInp.checked = true;
            defInp.onchange = defCheck;
            break;
        }
        case '' : { //String, String[]
            defInp.type = 'text';
            defInp.className = 'inp-value type-literal';
            break;
        }
        default : {
            defInp.type = 'text';
            defInp.className = 'inp-value type-literal';
            if (selVal.startsWith('{') || selVal.startsWith(',')) {
                defInp.value += selVal;
            } else {
                defInp.value = selVal;
            }
            break;
        }
    }
    defInp.focus();
}

async function addDynDef(dynId, verb) {
    let dynEl = $id(dynId);
    let reNameEl = $id(`inp-${dynId}`);
    let reName = reNameEl.value;

    if (!reName || (reName.trim() === '')) {
        reNameEl.focus();
        return;
    }

    let dynName = $id(`name-${dynId}`).value;
    let defName = dynName + reName;

    let opDef = await getApi(`/operation/${verb}`);

    let def = Object.keys(opDef.definitions).find(name => name === dynName);

    let defId = getNextId();
    let defSel = getSelectDef(defId, opDef.definitions[def]);

    let defEl = document.createElement('DIV');
    defEl.id = defId;
    defEl.className = 'tr-def';
    defEl.innerHTML = `<dd><label class="label-sel" for="sel-${defId}" class="dyn-name">${defName}</label><button onclick="removeDynDef('${defId}')">Remove</button>
<br>${defSel}<input id="name-${defId}" type="hidden" value="${defName}"></dd>`;

    dynEl.appendChild(defEl);
}

function removeDynDef(defId) {
    let defEl = $id(defId);
    defEl.parentElement.removeChild(defEl);
}


async function editDS(dsId, verb, kind) {
    let dsEl = $id(dsId);
    let nameEl = $id(`inp-${dsId}`);
    let dsRef = $id(`name-${dsId}`).value;
    let dsNames = nameEl.value;

    if (!dsNames || (dsNames.trim() === '')) {
        nameEl.focus();
        return;
    }

    nameEl.disabled = true;
    $id(`btn-props-${dsId}`).disabled = true;
    $id(`sel-${dsId}`).disabled = true;
    let nextColor = getDSColor(dsNames);
    dsEl.style.backgroundColor = nextColor;

    let dsDef = null;
    if (verb) {
        let opDef = await getApi(`/operation/${verb}`);
        if (dsRef) {
            dsDef = opDef[kind].streams[dsRef];
        } else {
            dsDef = opDef[kind].streams;
        }
    }
    let {columnar, generated} = dsDef ? dsDef : {columnar: false, generated: false};

    let names;
    if (dsRef) {
        names = [dsNames];
    } else {
        names = dsNames.split(',');
    }

    let msg = `<b>${dsNames}</b><small>${kind} properties</small>`;

    let editHtml = `<button class="btn-ds-save" onclick="applyDS('${kind}', '${dsRef}', '${dsNames}', ${columnar});cancelDS('${dsId}')">Apply</button><button class="btn-ds-cancel" onclick="cancelDS('${dsId}')">Cancel</button>${msg}<dl class="props">`;

    let editors = 0;
    names.forEach(dsName => {
        if ($id(`edit-${kind}-${dsName}`)) {
            return;
        }
        editors++;

        let ds = getDSTemplate();
        if (task.streams[dsName] && task.streams[dsName][kind]) {
            Object.entries(task.streams[dsName][kind])
                .forEach(([key, value]) => ds[kind][key] = value);
        }

        editHtml += `<div id="edit-${kind}-${dsName}">`;

        let partsId = `parts-${kind}-${dsName}`;
        let partsDef = {type: 'Integer'};
        let partsSel = getSelectDef(partsId, partsDef, ds[kind].partCount);
        editHtml += `<div id="${partsId}" class="tr-def"><dt><label for="inp-${partsId}">${dsName} part count</label></dt><dd>${partsSel}</dd></div>`;
        if (columnar) {
            let delId = `delimiter-${kind}-${dsName}`;
            let delDef = {type: 'String'};
            let delSel = getSelectDef(delId, delDef, ds[kind].delimiter);
            let colId = `columns-${kind}-${dsName}`;
            let colDef = (generated && generated.length) ? dsDef : {type: 'String'};
            let colSel = getSelectDef(colId, colDef, ds[kind].columns);
            editHtml += `<div id="${delId}" class="tr-def"><dt><label for="inp-${delId}">${dsName} column delimiter</label></dt><dd>${delSel}</dd></div>
<div id="${colId}" class="tr-def"><dt><label for="inp-${colId}">${dsName} column definitions</label></dt><dd>${colSel}</dd></div>`;
        }

        editHtml += '</div>';
    });
    editHtml += '</dl>';

    if (editors) {
        let editEl = document.createElement('DIV');
        editEl.id = `edit-${dsId}`;
        editEl.className = 'ds inter';
        editEl.style.backgroundColor = nextColor;

        editEl.innerHTML = editHtml;
        let canvas = $id('canvas-ds');
        switch (kind) {
            case 'input' : {
                let inputEditors = canvas.getElementsByClassName('ds input');
                inputEditors[inputEditors.length - 1].after(editEl);
                break;
            }
            case 'output' : {
                let outputEditors = canvas.getElementsByClassName('ds output');
                outputEditors[0].before(editEl);
                break;
            }
        }

        flashNew(editEl.id);
    }
}

function applyDS(kind, dsRef, dsName, columnar) {
    let dsNames;
    if (dsRef) {
        dsNames = [dsName];
    } else {
        dsNames = dsName.split(',');
    }

    let ds = [];
    dsNames.forEach(dsName => {
        if (!task.streams[dsName]) {
            task.streams[dsName] = getDSTemplate();
        }
        let dsKind = task.streams[dsName][kind];
        dsKind.partCount = $id(`inp-parts-${kind}-${dsName}`).value.trim();
        if (columnar) {
            dsKind.delimiter = $id(`inp-delimiter-${kind}-${dsName}`).value.trim();
            dsKind.columns = $id(`inp-columns-${kind}-${dsName}`).value.trim();
        }

        ds.push(dsKind);
    });

    updateVariables(ds);
}

function cancelDS(dsId) {
    let dsEl = $id(dsId);
    dsEl.style.backgroundColor = null;
    $id(`btn-props-${dsId}`).disabled = false;
    $id(`sel-${dsId}`).disabled = false;
    $id(`inp-${dsId}`).disabled = false;

    let editEl = $id(`edit-${dsId}`);
    editEl.parentElement.removeChild(editEl);
}

function tryRemoveDS(ds) {
    if (!ds) {
        return;
    }

    let dsList;

    if (Array.isArray(ds)) {
        dsList = ds;
    } else if (typeof ds === "string") {
        dsList = ds.split(',');
    } else {
        dsList = Object.keys(ds);
    }

    for (let dsName of dsList) {
        let hit = false;
        for (let i = 0; i < task.items.length; i++) {
            let op = task.items[i];
            if (op.verb) {
                hit = op.input.positional && op.input.positional.split(',').includes(dsName) ||
                    op.input.named && Object.values(op.input.named).includes(dsName) ||
                    op.output.positional && op.output.positional.split(',').includes(dsName) ||
                    op.output.named && Object.values(op.output.named).includes(dsName)
                ;
                if (hit) {
                    break;
                }
            }
        }
        if (!hit) {
            delete task.streams[dsName];
        }
    }
}


function updateVariables(obj) {
    let existingVars = Object.entries(variables).map(([k, _]) => k);
    let v = JSON.stringify(obj);
    Array.from(v.matchAll(REP_VAR), w => w[1].split(':', 2)[0]).forEach(v => {
        if (existingVars.indexOf(v) < 0) {
            variables[v] = '';
        }
    });

    initVariables();
}


function addDir(directive, idx) {
    let verb = directive.startsWith('$') ? directive.substring(1) : directive;
    verb = verb.includes('{') ? verb.substring(0, verb.indexOf('{')) : verb;

    let msg;
    let editable = true;
    switch (verb) {
        case 'IF': {
            msg = 'Execute following operations if the control variable is not empty';
            break;
        }
        case 'ITER': {
            msg = 'Loop through following operations for all values listed in the control variable. Each individual list item is accessible inside the loop via same control variable, and automatic {ITER} contains a unique random string';
            break;
        }
        case 'ELSE': {
            msg = 'If control variable of $IF or $ITER was empty, execute following operations instead';
            editable = false;
            break;
        }
        case 'END': {
            msg = 'End current $IF or $ITER scope';
            editable = false;
            break;
        }
        case 'LET': {
            msg = 'Make the control variable a list of all values from a specified data stream';
            break;
        }
        case 'METRICS': {
            msg = 'Calculate statistical metrics of the specified data streams';
            break;
        }
    }

    let dirEl = document.createElement('DIV');
    let dirId = getNextId();
    dirEl.id = dirId;

    let edit = '';
    if (editable) {
        edit = `<button id="btn-edit-${dirId}" onclick="editDir('${dirId}')">Edit</button><span id="btn-extra-${dirId}" class="btn-extra"></span>`;
    }
    dirEl.innerHTML = `<span class="btn-right"><span class="i">i<small>${msg}</small></span><button class="btn-remove" onclick="removeDir('${dirId}')" title="Remove">×</button></span>
<button class="btn-move" onclick="moveUp('${dirId}')" title="Move up">↑</button><button class="btn-move" onclick="moveDown('${dirId}')" title="Move down">↓</button>
<span class="dir-name">$${verb}</span>${edit}
<input type="hidden" id="verb-${dirId}" value="${verb}"><input type="hidden" id="position-${dirId}" class="task-position" value="${idx}">`;
    dirEl.className = 'dir task-item';

    let canvas = $id("canvas-task");

    let adapters = canvas.getElementsByClassName('adapter output');
    if (adapters.length) {
        adapters[0].before(dirEl);
    } else {
        canvas.insertAdjacentElement('beforeend', dirEl);
    }

    return dirId;
}

async function addNewDir(directive) {
    let idx = task.items.length;
    task.items.push(getDirTemplate(directive));

    let dirId = addDir(directive, idx);

    flashNew(dirId);
    if (!editor) {
        editDir(dirId);
    } else {
        describeDir(dirId);
    }
}

function describeDir(dirId) {
    let dirEl = $id(dirId);

    let dir = task.items[$id(`position-${dirId}`).value];

    let descrEl = document.createElement('PRE');
    descrEl.className = 'descr';
    descrEl.id = `descr-${dirId}`;
    descrEl.innerText = dir.directive;

    dirEl.appendChild(descrEl);
}

function editDir(dirId) {
    if (editor) {
        flashEditor();
        return;
    }

    let dirEl = $id(dirId);
    editor = dirEl;

    let descrEl = $id(`descr-${dirId}`);
    if (descrEl) {
        dirEl.removeChild(descrEl);
    }

    let btnEdit = $id(`btn-edit-${dirId}`);
    btnEdit.disabled = true;
    let verb = $id(`verb-${dirId}`).value;
    let dir = task.items[$id(`position-${dirId}`).value];

    let variable = '', value = '';
    let re = [...dir.directive.matchAll(REP_VAR)];
    if (re.length) {
        variable = re[0][1];
        if (variable.indexOf(':') > 0) {
            [variable, value] = variable.split(':', 2);
        }
    }

    let propsEl = document.createElement('DL');
    propsEl.className = 'props';
    propsEl.id = `props-${dirId}`;

    let buttonsEl = $id(`btn-extra-${dirId}`);
    buttonsEl.innerHTML = `<button onclick="applyDir('${dirId}');cancelDir('${dirId}');describeDir('${dirId}')">Apply</button><button onclick="cancelDir('${dirId}');describeDir('${dirId}')">Cancel</button>`;

    let propsHtml = '';
    switch (verb) {
        case 'IF':
        case 'ITER': {
            let varId = `control-${dirId}`;
            let varSel = getSelectDef(varId, {onlyVars: true}, variable);
            let defId = `value-${dirId}`;
            let defSel = getSelectDef(defId, {noVariables: true}, value);
            propsHtml = `<div class="tr-def" id="${varId}"><dt><label for="control-${dirId}">Control variable</label></dt>
<dd class="dir-control">${varSel}</dd></div>
<div class="tr-def" id="${defId}"><dt><label for="value-${defId}">Default value</label></dt>
<dd class="dir-value">${defSel}</dd></div>`;
            break;
        }
        case 'LET': {
            let varId = `control-${dirId}`;
            let varSel = getSelectDef(varId, {onlyVars: true}, variable);
            let dsId = `value-${dirId}`;
            let dsSel = getSelectDS(dsId, {noVariables: true}, value);
            propsHtml += `<div class="tr-def" id="${varId}"><dt><label for="control-${dirId}">Control variable</label></dt>
<dd class="dir-control">${varSel}</dd></div>
<div class="tr-def" id="${dsId}"><dt><label for="value-${dirId}">Data stream to source values</label></dt>
<dd class="dir-value">${dsSel}</dd></div>`;
            break;
        }
        case 'METRICS': {
            let dsId = `control-${dirId}`;
            let dsSel = getSelectDS(dsId, {noVariables: true, multiple: true}, variable);
            let predefId = `value-${dirId}`;
            let predefSel = getSelectDef(predefId, {
                noVariables: true,
                values: {'input': 'All task inputs', 'output': 'All task outputs'}
            }, value);
            propsHtml += `<div class="tr-def" id="${dsId}"><dt><label for="control-${dirId}">Data stream to instrument</label></dt>
<dd class="dir-control">${dsSel}</dd></div>
<div class="tr-def" id="${predefId}"><dt><label for="value-${dirId}">Task inputs and outputs</label></dt>
<dd class="dir-value">${predefSel}</dd></div>`;
            break;
        }
    }
    propsEl.innerHTML = propsHtml;

    dirEl.appendChild(propsEl);
}

function applyDir(dirId) {
    let dirEl = $id(dirId);
    let verb = $id(`verb-${dirId}`).value;

    let dir = getDirTemplate(verb);
    let variable = $id(`inp-control-${dirId}`).value.trim().toUpperCase();
    if (variable.startsWith('{')) {
        variable = variable.substring(1, variable.length - 2);
    }
    let value = $id(`inp-value-${dirId}`).value.trim();

    if (variable || value) {
        dir.directive += `{${variable}:${value}}`;
    }

    let idx = dirEl.getElementsByClassName('task-position')[0].value;
    task.items[idx] = dir;

    updateVariables({_: `{${variable}}`});
}

function cancelDir(dirId) {
    editor = false;

    let dirEl = $id(dirId);
    dirEl.removeChild($id(`props-${dirId}`));
    $id(`btn-edit-${dirId}`).disabled = false;
    $id(`btn-extra-${dirId}`).innerHTML = '';
}

function removeDir(dirId) {
    let dirEl = $id(dirId);

    if (editor === dirEl) {
        cancelDir(dirId);
    }

    let idx = dirEl.getElementsByClassName("task-position")[0].value;
    task.items.splice(idx, 1);

    dirEl.parentElement.removeChild(dirEl);

    renumberTaskItems();
}


function showDialog(message, content, buttonText, buttonFunction, ...args) {
    let modalEl = $id('modal');
    modalEl.style.display = 'flex';

    let headerEl = $id('dialog-message');
    headerEl.innerText = message;

    let textEl = $id('dialog-text');
    textEl.value = content;
    textEl.focus();

    let buttonEl = $id('dialog-button');
    let cancelEl = $id('dialog-cancel');
    if (buttonText) {
        buttonEl.innerText = buttonText;
        buttonEl.onclick = async function () {
            await buttonFunction(textEl.value, args);
        };
        buttonEl.style.display = null;
        cancelEl.innerText = 'Cancel';
    } else {
        buttonEl.style.display = 'none';
        cancelEl.innerText = 'Close';
    }
}

function cancelDialog() {
    let modalEl = $id('modal');
    modalEl.style.display = 'none';

    let headerEl = $id('dialog-message');
    headerEl.innerText = '';

    let buttonEl = $id('dialog-button');
    buttonEl.innerText = '';
    buttonEl.onclick = null;

    let textEl = $id('dialog-text');
    textEl.value = '';
}


function taskNew() {
    editor = false;
    nameCounter = 1;
    variables = {};
    initVariables();
    task = getTaskTemplate();

    $id('inp-task-prefix').value = task.prefix;

    initDefaultDS();

    $id('canvas-task').innerHTML = '';
}

function taskSetCode() {
    showDialog("This is current task's source code. You may manually edit it, copy, and even paste as JSON or .ini. " +
        "Changes in task's JSON will be validated by server before applied. If pasted in .ini format, it'll be converted on server to JSON before loaded",
        JSON.stringify(task, null, 2), 'Accept changes', taskValidateOnServer);
}

function taskSetPrefix() {
    let prefix = $id('inp-task-prefix').value.trim();
    if (prefix.endsWith('.')) {
        prefix = prefix.substring(0, prefix.length - 1);
    }
    if (prefix) {
        task.prefix = prefix;
    } else {
        delete task.prefix;
    }
}

function parseVariables(text) {
    cancelDialog();

    variables = {};
    text.split(/[\r\n]+/).forEach(p => {
        let pair = p.split('=', 2);
        if (pair.length === 2) {
            let variable = pair[0].trim().toUpperCase();
            if (variable) {
                variables[variable] = pair[1].trim();
            }
        }
    });

    initVariables();
}

function initVariables() {
    let varEl = $id('task-variables');
    varEl.innerHTML = Object.entries(variables).map(([k, v]) => `<option value="{${k}}">{${k}} ${v}</option>`).join('');
}

function taskSetVariables() {
    showDialog('Set task variables in form NAME=value, one variable per each line. You need to reopen all editors for them to appear in lists after that',
        Object.entries(variables).map(([k, v]) => `${k}=${v}`).join('\n'), 'Set', parseVariables);
}


async function taskValidate() {
    await taskValidateOnServer(JSON.stringify(task));
}

async function taskValidateOnServer(text) {
    text = text.trim();
    if (!text) {
        return;
    }

    if (text.startsWith('{')) {
        let resp = await fetch('/task/validate.json', {
            method: 'POST',
            body: text,
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if (resp.ok) {
            task = JSON.parse(text);
            await taskLoad();

            showDialog("Server successfully validated task code. This is its .ini format representation in the case you need it for further work outside this UI", await resp.text());
        } else {
            showDialog("Server returned an error. This is full text of error message", await resp.text());
        }
    } else {
        let prefix = text.split(/[\r\n]+/).map(line => line.split('=', 2)[0]).find(line => line.endsWith('.task.operations'));
        if (prefix) {
            prefix = prefix.substring(0, prefix.length - 16);
        }

        let resp = await fetch(`/task/validate.ini${prefix ? `?prefix=${prefix}` : ''}`, {
            method: 'POST',
            body: text,
            headers: {
                'Content-Type': 'text/plain'
            }
        });

        if (resp.ok) {
            cancelDialog();

            task = await resp.json();
            await taskLoad();
        } else {
            showDialog("Server returned an error. This is full text of error message", await resp.text());
        }
    }
}


async function taskLoad() {
    editor = false;

    $id('inp-task-prefix').value = task.prefix;

    initDefaultDS();

    $id('canvas-task').innerHTML = '';

    for (const ds of task.input) {
        let adapter = 'Hadoop';
        let path = task[`input.path.${ds}`];
        if (path && !path.startsWith('{')) {
            let form = new URLSearchParams();
            form.set('path', path);
            let resp = await fetch(`/adapter/forPath/input`, {
                method: 'POST',
                body: form,
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });
            adapter = await resp.text();
        }
        let adapterId = await addAdapter(adapter, 'input', ds);
        await describeAdapter(adapterId, 'input');
    }
    for (let i = 0; i < task.items.length; i++) {
        if (task.items[i].verb) {
            let opId = await addOp(task.items[i].verb, task.items[i].name, i);
            await describeOp(opId);
        } else {
            let dirId = await addDir(task.items[i].directive, i);
            describeDir(dirId);
        }
    }
    for (const ds of task.output) {
        let adapter = 'Hadoop';
        let path = task[`output.path.${ds}`];
        if (path && !path.startsWith('{')) {
            let form = new URLSearchParams();
            form.set('path', path);
            let resp = await fetch(`/adapter/forPath/output`, {
                method: 'POST',
                body: form,
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });
            adapter = await resp.text();
        }
        let adapterId = await addAdapter(adapter, 'output', ds);
        await describeAdapter(adapterId, 'output');
    }

    updateVariables(task);
}

async function taskRun() {

}
