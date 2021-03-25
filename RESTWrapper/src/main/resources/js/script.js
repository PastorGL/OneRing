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
        op: [],
        ds: [{
            name: '_default',
            input: {
                partCount: 1,
                delimiter: null
            },
            output: {
                partCount: 1,
                delimiter: null,
                path: null
            }
        }],
        tee: [],
        sink: [],
        prefix: "spark.meta"
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

async function initPackages() {
    let resp = await fetch('/package');
    let packages = await resp.json();

    let insList = '';
    let opsList = '';
    let outList = '';

    packages.forEach(pkg => {
        let li = `<li><span onclick="showPackage(this)"><span class="i">i<small>${pkg.descr}</small></span>${pkg.name}</span>`;
        if (pkg.ins.length) {
            insList += `${li}<ul class="package-list shown">${pkg.ins.map(inp => `<li><label><button onclick="addNewAdapter('${inp.name}', 'input')">${inp.name}</button><small>${inp.descr}</small></label></li>`).join('')}</ul></li>`;
        }
        if (pkg.ops.length) {
            opsList += `${li}<ul class="package-list">${pkg.ops.map(op => `<li><label><button onclick="addNewOp('${op.name}')">${op.name}</button><small>${op.descr}</small></label></li>`).join('')}</ul></li>`;
        }
        if (pkg.outs.length) {
            outList += `${li}<ul class="package-list shown">${pkg.outs.map(out => `<li><label><button onclick="addNewAdapter('${out.name}', 'output')">${out.name}</button><small>${out.descr}</small></label></li>`).join('')}</ul></li>`;
        }
    });

    let insEl = $id('list-inputs');
    insEl.innerHTML = insList;

    let opsEl = $id('list-operations');
    opsEl.innerHTML = opsList;

    let outEl = $id('list-outputs');
    outEl.innerHTML = outList;
}

function initDefaultDS() {
    let canvas = $id('canvas-ds');
    canvas.innerHTML = '';

    let defaultInputEl = document.createElement('DIV');
    defaultInputEl.className = 'ds input';
    defaultInputEl.id = 'default-input';
    defaultInputEl.innerHTML = `<span id="default-input-btn-extra" class="btn-extra"></span><button id="default-input-btn-props" onclick="editDefaultDS('input')">Edit</button> default <small>input properties</small>`;

    canvas.insertAdjacentElement('afterbegin', defaultInputEl);

    let defaultOutputEl = document.createElement('DIV');
    defaultOutputEl.className = 'ds output';
    defaultOutputEl.id = 'default-output';
    defaultOutputEl.innerHTML = `<span id="default-output-btn-extra" class="btn-extra"></span><button id="default-output-btn-props" onclick="editDefaultDS('output')">Edit</button> default <small>output properties</small>`;

    canvas.insertAdjacentElement('beforeend', defaultOutputEl);
}

function editDefaultDS(kind) {
    let defaultEl = $id(`default-${kind}`);
    $id(`default-${kind}-btn-props`).disabled = true;
    $id(`default-${kind}-btn-extra`).innerHTML = `<button onclick="applyDefaultDS('${kind}');cancelDefaultDS('${kind}')">Apply</button><button onclick="cancelDefaultDS('${kind}')">Cancel</button>`;

    let defaultEditEl = document.createElement('DL');
    defaultEditEl.className = 'props';
    defaultEditEl.id = `default-${kind}-props`;

    let defaultDS = task.ds.find(({name}) => name === '_default');

    let partsId = `default-${kind}-parts`;
    let partsDef = {type: 'Integer'};
    let partsSel = getSelectDef(partsId, partsDef, defaultDS[kind].partCount);
    let delId = `default-${kind}-delimiter`;
    let delDef = {type: 'String'};
    let delSel = getSelectDef(delId, delDef, defaultDS[kind].delimiter);
    let innerHtml = `<div id="${partsId}" class="tr-def"><dt>Part count</dt><dd>${partsSel}</dd></div>
<div id="${delId}" class="tr-def"><dt>Delimiter</dt><dd>${delSel}</dd></div>`;
    if (kind === 'output') {
        let pathId = `default-${kind}-path`;
        let pathDef = {type: 'String'};
        let pathSel = getSelectDef(pathId, pathDef, defaultDS.output.path);
        innerHtml += `<div id="${pathId}" class="tr-def"><dt>Path</dt><dd>${pathSel}</dd></div>`;
    }
    defaultEditEl.innerHTML = innerHtml;

    defaultEl.appendChild(defaultEditEl);
}

function applyDefaultDS(kind) {
    let defaultDS = task.ds.find(({name}) => name === '_default');

    let dsKind;
    switch (kind) {
        case 'input' : {
            dsKind = defaultDS.input;
            break;
        }
        case 'output' : {
            dsKind = defaultDS.output;
            dsKind.path = $id(`inp-default-${kind}-path`).value;
            break;
        }
    }

    dsKind.delimiter = $id(`inp-default-${kind}-delimiter`).value;
    dsKind.partCount = $id(`inp-default-${kind}-parts`).value;

    updateVariables(dsKind);
}

function cancelDefaultDS(kind) {
    let defaultEl = $id(`default-${kind}`);
    defaultEl.removeChild($id(`default-${kind}-props`));
    $id(`default-${kind}-btn-extra`).innerHTML = '';
    $id(`default-${kind}-btn-props`).disabled = false;
}

function showPackage(packageEl) {
    let pkgList = packageEl.parentElement.getElementsByClassName('package-list')[0];
    pkgList.style.display = (getComputedStyle(pkgList).display !== 'block') ? 'block' : 'none';
}

async function addAdapter(adapterName, kind, name) {
    let adapter = await getApi(`/adapter/${kind}/${adapterName}`);

    let adapterEl = document.createElement('DIV');
    let adapterId = getNextId();
    adapterEl.id = adapterId;
    adapterEl.innerHTML = `<span class="btn-right"><span class="i">i<small>${adapter.descr}</small></span><button class="btn-remove" onclick="removeAdapter('${adapterId}', '${kind}')" title="Remove">×</button></span>
<input id="inp-${adapterId}" class="inp-rename" value="${name}" readonly><button id="btn-edit-${adapterId}" class="btn-edit" onclick="editAdapter('${adapterId}', '${kind}')">Edit</button>
<span id="btn-extra-${adapterId}" class="btn-extra"></span>
<input type="hidden" id="verb-${adapterId}" value="${adapterName}"><input type="hidden" id="name-${adapterId}" value="${name}">`;
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

    return adapterEl;
}

async function addNewAdapter(adapterName, kind) {
    let name = getNextName(adapterName);

    switch (kind) {
        case 'input': {
            task.sink.unshift(name);
            break;
        }
        case 'output' : {
            task.tee.push(name);
            break;
        }
    }
    task.ds.push(getDSTemplate(name));

    let adapterEl = await addAdapter(adapterName, kind, name);
    flashNew(adapterEl);
    if (!editor) {
        await editAdapter(adapterEl.id, kind);
    }
}

function renumberTaskItems() {
    let taskItems = document.getElementsByClassName('task-item');
    for (let idx = 0; idx < taskItems.length; idx++) {
        let positionEl = taskItems[idx].getElementsByClassName('task-position')[0];
        positionEl.value = idx;
    }
}

async function addOp(verb, name) {
    let op = await getApi(`/operation/${verb}`);

    let opEl = document.createElement('DIV');
    let opId = getNextId();
    opEl.id = opId;

    opEl.innerHTML = `<span class="btn-right">${verb}<span class="i">i<small>${op.descr}</small></span><button class="btn-remove" onclick="removeOp('${opId}')" title="Remove">×</button></span>
<button class="btn-move" onclick="moveUp('${opId}')" title="Move up">↑</button><button class="btn-move" onclick="moveDown('${opId}')" title="Move down">↓</button>
<input id="inp-${opId}" class="inp-rename" value="${name}" readonly><button id="btn-edit-${opId}" class="btn-edit" onclick="editOp('${opId}')">Edit</button>
<span id="btn-extra-${opId}" class="btn-extra"></span>
<input type="hidden" id="verb-${opId}" value="${verb}"><input type="hidden" id="name-${opId}" value="${name}"><input id="position-${opId}" type="hidden" class="task-position">`;
    opEl.className = 'op task-item';

    let canvas = $id("canvas-task");

    let adapters = canvas.getElementsByClassName('adapter output');
    if (adapters.length) {
        adapters[0].before(opEl);
    } else {
        canvas.insertAdjacentElement('beforeend', opEl);
    }

    return opEl;
}

async function addNewOp(verb) {
    let name = getNextName(verb);
    task.op.push(getOpTemplate(name, verb));

    let opEl = await addOp(verb, name);
    renumberTaskItems();

    flashNew(opEl);
    if (!editor) {
        await editOp(opEl.id);
    }
}

function moveUp(elId) {
    let el = $id(elId);
    let prevEl = el.previousElementSibling;
    if (prevEl && !prevEl.classList.contains('adapter')) {
        prevEl.before(el);

        let position = el.getElementsByClassName('task-position')[0].value;
        let ti = task.op.splice(position, 1);
        task.op.splice(position - 1, 0, ti);
    }

    renumberTaskItems();
}

function moveDown(elId) {
    let el = $id(elId);
    let nextEl = el.nextElementSibling;
    if (nextEl && !nextEl.classList.contains('adapter')) {
        nextEl.after(el);

        let position = el.getElementsByClassName('task-position')[0].value;
        let ti = task.op.splice(position, 1);
        task.op.splice(position + 1, 0, ti);
    }

    renumberTaskItems();
}

function getSelectDS(dsId, value = '') {
    let selHtml = `<select id="sel-${dsId}" class="sel-ds" onchange="selectDS('${dsId}', this)"><optgroup label="Task data streams">`;

    let literal = true;
    task.ds.forEach(({name}) => {
        if (name !== '_default') {
            if (name === value) {
                selHtml += `<option selected>${name}</option>`;
                literal = false;
            } else {
                selHtml += `<option>${name}</option>`;
            }
        }
    });

    selHtml += '</optgroup><optgroup label="Literal or add {VARIABLE}">';

    Object.entries(variables).forEach(([k, v]) => {
        if (value === `{${k}}`) {
            selHtml += `<option value="{${k}}" selected>{${k}} ${v}</option>`;
            literal = false;
        } else {
            selHtml += `<option value="{${k}}">{${k}} ${v}</option>`;
        }
    });

    if (literal) {
        selHtml += `<option value="" selected>Literal</option>`;
    } else {
        selHtml += `<option value="">Literal</option>`;
    }

    selHtml += `</optgroup></select><input id="inp-${dsId}" class="inp-value type-literal" value="${value}">`;
    return selHtml;
}

function selectDS(dsId, selEl) {
    let selVal = selEl.value;
    let dsInp = $id(`inp-${dsId}`);

    switch (selVal) {
        case '' : {
            break;
        }
        default : {
            if (selVal.startsWith('{')) {
                dsInp.value += selVal;
            } else {
                dsInp.value += dsInp.value ? `,${selVal}` : selVal;
            }
            break;
        }
    }
    dsInp.focus();
}

function getSelectDef(defId, def, value = '') {
    let selHtml = `<select id="sel-${defId}" class="sel-def" onchange="selectDef('${defId}', this)">`;

    let literal = true;
    let inputProps = 'class="inp-value type-literal" type="text"';
    let withVars = true;
    if (def) {
        if (def.defaults) {
            let defaults = def.defaults.name;
            if (defaults === null) {
                defaults = '';
            }
            if ((value === '') || (value === null)) {
                value = defaults;
                literal = false;
            }
            selHtml += `<optgroup label="Set to default value"><option value="${defaults}" ${(defaults === value) ? 'selected' : ''}>${def.defaults.name} ${def.defaults.descr}</option></optgroup>`;
        }

        if (def.generated && def.generated.length) {
            selHtml += `<optgroup label="Add generated value">`;
            def.generated.forEach(g => {
                if (value === g.name) {
                    selHtml += `<option value=",${g.name}" selected>${g.name} ${g.descr}</option>`;
                    literal = false;
                } else {
                    selHtml += `<option value=",${g.name}">${g.name} ${g.descr}</option>`;
                }
            });
            selHtml += '</optgroup>';
        } else if (def.values && def.values.length) {
            selHtml += `<optgroup label="One of ${def.type}">`;
            def.values.forEach(v => {
                if (value === v.name) {
                    selHtml += `<option value="${v.name}" selected>${v.name} ${v.descr}</option>`;
                    literal = false;
                } else {
                    selHtml += `<option value="${v.name}">${v.name} ${v.descr}</option>`;
                }
            });
            selHtml += '</optgroup>';
        } else if (def.type && !def.type.startsWith('String')) {
            selHtml += `<optgroup label="Of type ${def.type}">`;
            let match = false;
            switch (def.type) {
                case `Double` : {
                    if (value && value.match(REP_DOUBLE)) {
                        inputProps = 'class="inp-value type-number" type="number" step="any"';
                        literal = false;
                    }
                    break;
                }
                case 'Byte':
                case 'Integer':
                case 'Long': {
                    if (value && value.match(REP_INTEGER)) {
                        inputProps = 'class="inp-value type-number" type="number" step="1"';
                        literal = false;
                    }
                    break;
                }
                case 'Boolean' : {
                    if ((value === 'true') || (value === 'false')) {
                        inputProps = `class="inp-value type-boolean" type="checkbox" onchange="defCheck" value="${value}"`;
                        if (value === 'true') {
                            inputProps += ' checked';
                        }
                        literal = false;
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
        withVars = !def.noVariables;
    }

    selHtml += `<optgroup label="Literal${withVars ? ' or add {VARIABLE}' : ''}">`;
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

    selHtml += `</optgroup></select><input id="inp-${defId}" ${inputProps} value="${value}">`;
    return selHtml;
}

function defCheck(ev) {
    if (ev.target.type === 'checkbox') {
        ev.target.value = ev.target.checked + '';
    }
    ev.stopPropagation();
}

function selectDef(defId, selEl) {
    let selVal = selEl.value;
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

async function getApi(apiUrl) {
    if (apiCache.hasOwnProperty(apiUrl)) {
        return apiCache[apiUrl];
    }

    let resp = await fetch(apiUrl);
    let obj = await resp.json();
    apiCache[apiUrl] = obj;

    return obj;
}

async function editOp(opId) {
    if (editor) {
        flashEditor();
        return;
    }

    let opEl = $id(opId);
    editor = opEl;

    let btnEdit = $id(`btn-edit-${opId}`);
    btnEdit.disabled = true;
    let verb = $id(`verb-${opId}`).value;
    let op = task.op[$id(`position-${opId}`).value];

    let opDef = await getApi(`/operation/${verb}`);

    let propsEl = document.createElement('DL');
    propsEl.className = 'props';
    propsEl.id = `props-${opId}`;

    let buttonsEl = $id(`btn-extra-${opId}`);
    buttonsEl.innerHTML = `<button onclick="applyOp('${opId}');cancelOp('${opId}')">Apply</button><button onclick="cancelOp('${opId}')">Cancel</button>`;

    let propsHtml = '';
    if (opDef.positionalMin !== null) {
        let dsId = getNextId();
        let dsSel = getSelectDS(dsId, op.inputs.positional.join(','));
        propsHtml += `<div id="${dsId}" class="tr-input"><dt><span class="btn-right"><button onclick="editDS('${dsId}', '${verb}', 'input')" id="btn-props-${dsId}">Properties</button></span>
<label for="sel-${dsId}">At least ${opDef.positionalMin} positional inputs of type ${opDef.positionalInputs.type}</label></dt>
<dd>${dsSel}<input type="hidden" id="name-${dsId}" value=""></dd></div>`;
    } else if (opDef.positionalInputs !== null) {
        let dsId = getNextId();
        let dsSel = getSelectDS(dsId, op.inputs.positional.join(','));
        propsHtml += `<div id="${dsId}" class="tr-input"><dt><span class="btn-right"><button onclick="editDS('${dsId}', '${verb}', 'input')" id="btn-props-${dsId}">Properties</button></span>
<label for="sel-${dsId}">Positional input(s) of type ${opDef.positionalInputs.type}</label></dt>
<dd>${dsSel}<input type="hidden" id="name-${dsId}" value=""></dd></div>`;
    } else {
        opDef.namedInputs.forEach(ni => {
            let dsId = getNextId();
            let found = op.inputs.named.find(({name}) => name === ni.name);
            let dsSel = getSelectDS(dsId, found ? found.value : '');
            propsHtml += `<div id="${dsId}" class="tr-input"><dt><span class="btn-right"><button onclick="editDS('${dsId}', '${verb}', 'input')" id="btn-props-${dsId}">Properties</button></span>
<label for="sel-${dsId}">Input ${ni.name} of type ${ni.type}<small>${ni.descr}</small></label></dt>
<dd>${dsSel}<input type="hidden" id="name-${dsId}" value="${ni.name}"></dd></div>`;
        });
    }
    if (opDef.mandatoryParameters.length) {
        opDef.mandatoryParameters.forEach(def => {
            let defId = getNextId();
            let found = op.definitions.find(({name}) => name === def.name);
            let defSel = getSelectDef(defId, def, found ? found.value : '');
            propsHtml += `<div id="${defId}" class="tr-def"><dt><label for="sel-${defId}">${def.name}<small>${def.descr}</small></label></dt>
<dd id="${defId}">${defSel}<input id="name-${defId}" type="hidden" value="${def.name}"></dd></div>`;
        });
    }
    if (opDef.optionalParameters.length) {
        opDef.optionalParameters.forEach(def => {
            let defId = getNextId();
            let found = op.definitions.find(({name}) => name === def.name);
            let defSel = getSelectDef(defId, def, found ? found.value : '');
            propsHtml += `<div id="${defId}" class="tr-def"><dt><label for="sel-${defId}">${def.name}<small>${def.descr}</small></label></dt>
<dd id="${defId}">${defSel}<input id="name-${defId}" type="hidden" value="${def.name}"></dd></div>`;
        });
    }
    if (opDef.dynamicParameters.length) {
        opDef.dynamicParameters.forEach(def => {
            let dynId = getNextId();
            propsHtml += `<div id="${dynId}" class="tr-dyn"><dt><label for="inp-${dynId}">${def.name}<small>${def.descr}</small></label></dt>
<dd><input id="name-${dynId}" type="hidden" value="${def.name}"><input id="inp-${dynId}" class="inp-rename"><button onclick="addDynDef('${dynId}', '${verb}')">Add</button></dd>`;
            let found = op.definitions.filter(({name}) => name.startsWith(def.name));
            found.forEach(dyn => {
                let defId = getNextId();
                let defSel = getSelectDef(defId, def, dyn.value);
                let defName = dyn.name.substring(def.name.length);
                propsHtml += `<div id="${defId}" class="tr-def"><dd><label for="sel-${defId}" class="dyn-name">${defName}</label><button onclick="removeDynDef('${defId}')">Remove</button>
<br>${defSel}<input id="name-${defId}" type="hidden" value="${defName}"></dd></div>`;
            });
            propsHtml += '</div>';
        });
    }
    if (opDef.positionalOutputs !== null) {
        let dsId = getNextId();
        let dsSel = getSelectDS(dsId, op.outputs.positional.join(','));
        propsHtml += `<div id="${dsId}" class="tr-output"><dt><span class="btn-right"><button onclick="editDS('${dsId}', '${verb}', 'output')" id="btn-props-${dsId}">Properties</button></span>
<label for="sel-${dsId}">Positional output(s) of type ${opDef.positionalOutputs.type}</label></dt>
<dd>${dsSel}<input type="hidden" id="name-${dsId}" value=""></dd></div>`;
    } else {
        opDef.namedOutputs.forEach(no => {
            let dsId = getNextId();
            let found = op.outputs.named.find(({name}) => name === no.name);
            let dsSel = getSelectDS(dsId, found ? found.value : '');
            propsHtml += `<div id="${dsId}" class="tr-output"><dt><span class="btn-right"><button onclick="editDS('${dsId}', '${verb}', 'output')" id="btn-props-${dsId}">Properties</button></span>
<label for="sel-${dsId}">Output ${no.name} of type ${no.type}<small>${no.descr}</small></label></dt>
<dd>${dsSel}<input type="hidden" id="name-${dsId}" value="${no.name}"></dd></div>`;
        });
    }
    propsEl.innerHTML = propsHtml;

    opEl.appendChild(propsEl);

    let reNameEl = $id(`inp-${opId}`);
    reNameEl.readOnly = false;
    reNameEl.focus();
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

    let op = await getApi(`/operation/${verb}`);

    let def = op.dynamicParameters.find(({name}) => name === dynName);

    let defId = getNextId();
    let defSel = getSelectDef(defId, def);

    let defEl = document.createElement('DIV');
    defEl.id = defId;
    defEl.className = 'tr-def';
    defEl.innerHTML = `<dd><label for="sel-${defId}" class="dyn-name">${defName}</label><button onclick="removeDynDef('${defId}')">Remove</button>
<br>${defSel}<input id="name-${defId}" type="hidden" value="${defName}"></dd>`;

    dynEl.appendChild(defEl);
}

function removeDynDef(defId) {
    let defEl = $id(defId);
    defEl.parentElement.removeChild(defEl);
}

async function editAdapter(adapterId, kind) {
    if (editor) {
        flashEditor();
        return;
    }

    let adapterEl = $id(adapterId);
    editor = adapterEl;

    let btnEdit = $id(`btn-edit-${adapterId}`);
    btnEdit.disabled = true;
    let verb = $id(`verb-${adapterId}`).value;

    $id(`btn-extra-${adapterId}`).innerHTML = `<button onclick="applyAdapter('${adapterId}', '${kind}');cancelAdapter('${adapterId}')">Apply</button><button onclick="cancelAdapter('${adapterId}')">Cancel</button>`;

    let adapter = await getApi(`/adapter/${kind}/${verb}`);

    let propsEl = document.createElement('DL');
    propsEl.className = 'props';
    propsEl.id = `props-${adapterId}`;

    let dsName = $id(`name-${adapterId}`).value;
    let ds = task.ds.find(({name}) => name === dsName);

    let pathId = `path-${adapterId}`;
    let pathDef = {
        type: 'String',
        defaults: {
            name: adapter.proto,
            descr: 'Path specification'
        }
    };
    let pathSel = getSelectDef(pathId, pathDef, ds[kind].path);
    let propsHtml = `<div id="${pathId}" class="tr-def"><dt>Path<small>${adapter.proto}</small></dt><dd>${pathSel}</dd></div>`;
    if (kind === 'input') {
        let schemaId = `schema-${adapterId}`;
        let schemaDef = {type: 'String'};
        let schemaSel = getSelectDef(schemaId, schemaDef, ds.input.sinkSchema);
        propsHtml += `<div id="${schemaId}" class="tr-def"><dt>Sink schema</dt><dd>${schemaSel}</dd></div>`;
    }
    propsEl.innerHTML = propsHtml;

    adapterEl.appendChild(propsEl);

    let reNameEl = $id(`inp-${adapterId}`);
    reNameEl.readOnly = false;
    reNameEl.focus();
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

    let op = await getApi(`/operation/${verb}`);
    let dsDef;
    switch (kind) {
        case 'input' : {
            dsDef = (dsRef ? op.namedInputs.find(({name}) => name === dsRef) : op.positionalInputs);
            break;
        }
        case 'output' : {
            dsDef = (dsRef ? op.namedOutputs.find(({name}) => name === dsRef) : op.positionalOutputs);
            break;
        }
    }
    let {columnar, generated} = dsDef;

    let names;
    if (dsRef) {
        names = [dsNames];
    } else {
        names = dsNames.split(',');
    }

    let msg = `${dsNames}<small>${kind} properties</small>`;

    let editHtml = `<button class="btn-ds-save" onclick="applyDS('${verb}', '${kind}', '${dsRef}', '${dsNames}', ${columnar});cancelDS('${dsId}')">Apply</button><button class="btn-ds-cancel" onclick="cancelDS('${dsId}')">Cancel</button>${msg}<dl class="props">`;

    let editors = 0;
    names.forEach(dsName => {
        if ($id(`edit-${dsName}`)) {
            return;
        }
        editors++;

        let ds = task.ds.find(({name}) => name === dsName);
        if (!ds) {
            ds = getDSTemplate(dsName);
        }

        editHtml += `<div id="edit-${dsName}">`;

        let partsId = `parts-${dsName}`;
        let partsDef = {type: 'Integer'};
        let partsSel = getSelectDef(partsId, partsDef, ds[kind].partCount);
        editHtml += `<div id="${partsId}" class="tr-def"><dt>${dsName} part count</dt><dd>${partsSel}</dd></div>`;
        if (columnar) {
            let colId = `columns-${dsName}`;
            let colDef = (generated && generated.length) ? dsDef : {type: 'String'};
            let colSel = getSelectDef(colId, colDef, ds[kind].columns ? ds[kind].columns.join(',') : null);
            let delId = `delimiter-${dsName}`;
            let delDef = {type: 'String'};
            let delSel = getSelectDef(delId, delDef, ds[kind].delimiter);
            editHtml += `<div id="${delId}" class="tr-def"><dt>${dsName} column delimiter</dt><dd>${delSel}</dd></div>
<div id="${colId}" class="tr-def"><dt>${dsName} column definitions</dt><dd>${colSel}</dd></div>`;
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

        flashNew(editEl);
    }
}

function getDSTemplate(dsName) {
    return {
        name: dsName,
        input: {
            columns: [],
            delimiter: null,
            partCount: null,
            path: null
        },
        output: {
            columns: [],
            delimiter: null,
            partCount: null,
            path: null
        }
    };
}

function applyDS(verb, kind, dsRef, dsName, columnar) {
    let dsNames;
    if (dsRef) {
        dsNames = [dsName];
    } else {
        dsNames = dsName.split(',');
    }

    let ds = [];
    dsNames.forEach(dsName => {
        let idx = task.ds.length ? task.ds.findIndex(({name}) => name === dsName) : -1;
        if (idx < 0) {
            idx = task.ds.length;
            task.ds[idx] = getDSTemplate(dsName);
        }
        let dsTemplate = task.ds[idx];

        let dsKind;
        switch (kind) {
            case 'input' : {
                dsKind = dsTemplate.input;
                break;
            }
            case 'output' : {
                dsKind = dsTemplate.output;
                break;
            }
        }

        if (columnar) {
            dsKind.columns = $id(`inp-columns-${dsName}`).value.split(',');
            dsKind.delimiter = $id(`inp-delimiter-${dsName}`).value;
        }
        dsKind.partCount = $id(`inp-parts-${dsName}`).value;

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

function applyAdapter(adapterId, kind) {
    let adapterEl = $id(adapterId);
    let nameEl = $id(`name-${adapterId}`);
    let adapterName = nameEl.value;
    let reName = $id(`inp-${adapterId}`).value;

    let ds = task.ds.find(({name}) => name === adapterName);
    ds.name = reName;

    let path = $id(`inp-path-${adapterId}`).value;
    switch (kind) {
        case 'input' : {
            let idx = task.sink.length ? task.sink.findIndex(name => name === adapterName) : -1;
            if (idx >= 0) {
                task.sink.splice(idx, 1);
            }
            task.sink.push(reName);
            ds.input.path = path;
            ds.input.sinkSchema = $id(`inp-schema-${adapterId}`).value.split(',');
            break;
        }
        case 'output' : {
            let idx = task.tee.length ? task.tee.findIndex(name => name === adapterName) : -1;
            if (idx >= 0) {
                task.tee.splice(idx, 1);
            }
            task.tee.push(reName);
            ds.output.path = path;
            break;
        }
    }
    nameEl.value = reName;

    adapterEl.style.backgroundColor = getDSColor(reName);

    updateVariables(ds);
}

function getOpTemplate(opName, verb) {
    return {
        name: opName,
        verb: verb,
        definitions: [],
        inputs: {
            positional: [],
            named: []
        },
        outputs: {
            positional: [],
            named: []
        }
    };
}

function updateDS(dataStreams) {
    let taskDataStreams = task.ds.map(({name}) => name);
    dataStreams.forEach(n => {
        if (taskDataStreams.indexOf(n) < 0) {
            task.ds.push(getDSTemplate(n));
        }
    });
}

function applyOp(opId) {
    let opEl = $id(opId);
    let nameEl = $id(`name-${opId}`);
    let reName = $id(`inp-${opId}`).value;
    let verb = $id(`verb-${opId}`).value;

    let op = getOpTemplate(reName, verb);
    let idx = opEl.getElementsByClassName('task-position')[0].value;
    task.op[idx] = op;
    nameEl.value = reName;

    let dsSaveButtons = $id('canvas-ds').getElementsByClassName('btn-ds-save');
    Array.from(dsSaveButtons).forEach(btn => btn.click());

    let dataStreams = new Set();

    let opIns = opEl.getElementsByClassName("tr-input");
    Array.from(opIns).forEach(tr => {
        let dsId = tr.id;
        let dsRef = $id(`name-${dsId}`).value;
        let dsName = $id(`inp-${dsId}`).value;
        if (dsRef) {
            op.inputs.named.push(({
                name: dsRef,
                value: dsName
            }));
            dataStreams.add(dsName);
        } else {
            let dsNames = dsName.split(',');
            op.inputs.positional = dsNames;
            dsNames.forEach(n => dataStreams.add(n));
        }
    });

    let opOuts = opEl.getElementsByClassName("tr-output");
    Array.from(opOuts).forEach(tr => {
        let dsId = tr.id;
        let dsRef = $id(`name-${dsId}`).value;
        let dsName = $id(`inp-${dsId}`).value;
        if (dsRef) {
            op.outputs.named.push(({
                name: dsRef,
                value: dsName
            }));
            dataStreams.add(dsName);
        } else {
            let dsNames = dsName.split(',');
            op.outputs.positional = dsNames;
            dsNames.forEach(n => dataStreams.add(n));
        }
    });

    updateDS(dataStreams);

    let opDefs = opEl.getElementsByClassName("tr-def");
    Array.from(opDefs).forEach(tr => {
        let defId = tr.id;
        let defName = $id(`name-${defId}`).value;
        let defVal = $id(`inp-${defId}`).value;
        op.definitions.push(({
            name: defName,
            value: ((defVal === '') ? null : defVal)
        }));
    });

    updateVariables(op);
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

function cancelDir(dirId) {
    editor = false;

    let dirEl = $id(dirId);
    dirEl.removeChild($id(`props-${dirId}`));
    $id(`btn-edit-${dirId}`).disabled = false;
    $id(`btn-extra-${dirId}`).innerHTML = '';
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

function removeDir(dirId) {
    let dirEl = $id(dirId);

    if (editor === dirEl) {
        cancelDir(dirId);
    }

    let idx = dirEl.getElementsByClassName("task-position")[0].value;
    task.op.splice(idx, 1);

    dirEl.parentElement.removeChild(dirEl);

    renumberTaskItems();
}

function removeOp(opId) {
    let opEl = $id(opId);

    if (editor === opEl) {
        cancelOp(opId);
    }

    let idx = opEl.getElementsByClassName("task-position")[0].value;
    task.op.splice(idx, 1);

    opEl.parentElement.removeChild(opEl);

    renumberTaskItems();
}

function removeAdapter(adapterId, kind) {
    let adapterEl = $id(adapterId);

    if (editor === adapterEl) {
        cancelAdapter(adapterId);
    }

    let adapterName = $id(`name-${adapterId}`).value;
    adapterEl.parentElement.removeChild(adapterEl);

    switch (kind) {
        case 'input' : {
            let idx = task.sink.length ? task.sink.findIndex(name => name === adapterName) : -1;
            if (idx >= 0) {
                task.sink.splice(idx, 1);
            }
            break;
        }
        case 'output' : {
            let idx = task.tee.length ? task.tee.findIndex(name => name === adapterName) : -1;
            if (idx >= 0) {
                task.tee.splice(idx, 1);
            }
            break;
        }
    }
}

function addDir(directive) {
    let msg;
    let editable = true;
    switch (directive) {
        case 'IF': {
            msg = 'Execute following operations if control variable is not empty';
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
            msg = 'Make the control variable a list of all values from specified data stream';
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
<span class="dir-name">$${directive}</span>${edit}
<input type="hidden" id="verb-${dirId}" value="${directive}"><input type="hidden" id="position-${dirId}" class="task-position">`;
    dirEl.className = 'dir task-item';

    let canvas = $id("canvas-task");

    let adapters = canvas.getElementsByClassName('adapter output');
    if (adapters.length) {
        adapters[0].before(dirEl);
    } else {
        canvas.insertAdjacentElement('beforeend', dirEl);
    }

    return dirEl;
}

async function addNewDir(directive) {
    task.op.push(getDirTemplate(directive));

    let dirEl = addDir(directive);
    renumberTaskItems();

    flashNew(dirEl);
    if (!editor) {
        editDir(dirEl.id);
    }
}

function getDirTemplate(directive) {
    return {
        verb: directive,
        variable: null,
        value: null
    };
}

function applyDir(dirId) {
    let dirEl = $id(dirId);
    let verb = $id(`verb-${dirId}`).value;

    let dir = getDirTemplate(verb);
    let variable = $id(`inp-control-${dirId}`).value;
    if (variable.startsWith('{')) {
        variable = variable.substring(1, variable.length - 2);
    }
    dir.variable = variable;
    dir.value = $id(`inp-value-${dirId}`).value;

    let idx = dirEl.getElementsByClassName('task-position')[0].value;
    task.op[idx] = dir;

    updateVariables({_: `{${variable}}`});
}

function flashEditor() {
    if (!editor.classList.contains('flash-editor')) {
        editor.classList.add('flash-editor');
        editor.scrollIntoView();
        setTimeout(removeFlash, 1100, editor, 'flash-editor');
    }
}

function flashNew(el) {
    el.classList.add('flash-new');
    el.scrollIntoView();
    setTimeout(removeFlash, 1100, el, 'flash-new');
}

function removeFlash(el, flash) {
    el.classList.remove(flash);
}

function editDir(dirId) {
    if (editor) {
        flashEditor();
        return;
    }

    let dirEl = $id(dirId);

    let verb = $id(`verb-${dirId}`).value;
    let dir = task.op[$id(`position-${dirId}`).value];

    let propsEl = document.createElement('DL');
    propsEl.className = 'props';
    propsEl.id = `props-${dirId}`;

    let buttonsEl = $id(`btn-extra-${dirId}`);
    buttonsEl.innerHTML = `<button onclick="applyDir('${dirId}');cancelDir('${dirId}')">Apply</button><button onclick="cancelDir('${dirId}')">Cancel</button>`;

    let propsHtml = '';
    switch (verb) {
        case 'IF':
        case 'ITER': {
            let varId = `control-${dirId}`;
            let varSel = getSelectDef(varId, null, dir.variable);
            let defId = `value-${dirId}`;
            let defSel = getSelectDef(defId, {
                noVariables: true
            }, dir.value);
            propsHtml = `<div class="tr-def" id="${varId}"><dt>Control variable</dt>
<dd class="dir-control">${varSel}</dd></div>
<div class="tr-def" id="${defId}"><dt>Default value</dt>
<dd class="dir-value">${defSel}</dd></div>`;
            break;
        }
        case 'LET': {
            let varId = `control-${dirId}`;
            let varSel = getSelectDef(varId, null, dir.variable);
            let dsId = `value-${dirId}`;
            let dsSel = getSelectDS(dsId, dir.value);
            propsHtml += `<div class="tr-def" id="${varId}"><dt>Control variable</dt>
<dd class="dir-control">${varSel}</dd></div>
<div class="tr-def" id="${dsId}"><dt>Data stream to source values</dt>
<dd class="dir-value">${dsSel}</dd></div>`;
            break;
        }
        case 'METRICS': {
            let dsId = `control-${dirId}`;
            let dsSel = getSelectDS(dsId, dir.variable);
            let predefId = `value-${dirId}`;
            let predefSel = getSelectDef(predefId, {
                noVariables: true,
                defaults: [{name: 'sink', descr: 'All task inputs'}, {name: 'tee', descr: 'All task outputs'}]
            }, dir.value);
            propsHtml += `<div class="tr-def" id="${dsId}"><dt>Data stream to instrument</dt>
<dd class="dir-control">${dsSel}</dd></div>
<div class="tr-def" id="${predefId}"><dt>Task inputs and outputs</dt>
<dd class="dir-value">${predefSel}</dd></div>`;
            break;
        }
    }
    propsEl.innerHTML = propsHtml;

    dirEl.appendChild(propsEl);
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

function showDialog(message, content, buttonText, buttonFunction) {
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
            await buttonFunction(textEl.value);
        };
        buttonEl.style.display = null;
        cancelEl.innerText = 'Cancel';
    } else {
        buttonEl.style.display = 'none';
        cancelEl.innerText = 'Close';
    }
}

function parseVariables(text) {
    cancelDialog();

    variables = {};
    text.split(/[\r\n]+/).forEach(p => {
        let pair = p.split('=', 2);
        variables[pair[0]] = pair[1];
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

function taskSetCode() {
    showDialog("This is current task's source code. You may manually edit it, copy, and even paste as JSON or .ini. " +
        "Changes in task's JSON will be validated by server before applied. If pasted in .ini format, it'll be converted on server to JSON before loaded",
        JSON.stringify(task, null, 2), 'Accept changes', taskValidateOnServer);
}

function taskSetPrefix() {
    task.prefix = $id('inp-task-prefix').value;
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
            taskLoad();

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
            taskLoad();
        } else {
            showDialog("Server returned an error. This is full text of error message", await resp.text());
        }
    }
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

async function taskLoad() {
    editor = false;

    $id('inp-task-prefix').value = task.prefix;

    initDefaultDS();

    $id('canvas-task').innerHTML = '';

    for (const ds of task.sink) {
        await addAdapter('HadoopInput', 'input', ds);
    }
    for (let i = 0; i < task.op.length; i++) {
        let {verb, name = null} = task.op[i];
        if (name) {
            await addOp(verb, name, i);
        } else {
            await addDir(verb, i);
        }
    }
    renumberTaskItems();
    for (const ds of task.tee) {
        await addAdapter('HadoopOutput', 'output', ds);
    }

    updateVariables(task);
}

async function taskRun() {

}
