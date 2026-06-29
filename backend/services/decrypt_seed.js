const fs = require('fs');
const vm = require('vm');

// Load and eval seedForNode.js in a sandbox with CryptoJS as global
const seedCode = fs.readFileSync(__dirname + '/seedForNode.js', 'utf8');
const sandbox = { Math: Math, undefined: undefined };
vm.createContext(sandbox);
vm.runInContext(seedCode, sandbox);
const CryptoJS = sandbox.CryptoJS;

const encrypted = process.argv[2] || fs.readFileSync('/dev/stdin', 'utf8').trim();
if (!encrypted) { console.error('No input'); process.exit(1); }

function string_to_utf8_hex_string(str) {
    let hex = '';
    for (let i = 0; i < str.length; i++) {
        hex += str.charCodeAt(i).toString(16).padStart(2, '0');
    }
    return hex;
}

const keyText = "ILOGEN.COMGCSEED";
const keyHex = string_to_utf8_hex_string(keyText);
const key = CryptoJS.enc.Hex.parse(keyHex);

const decrypted = CryptoJS.SEED.decrypt(encrypted, key, { iv: keyText });
console.log(decrypted.toString(CryptoJS.enc.Utf8));
