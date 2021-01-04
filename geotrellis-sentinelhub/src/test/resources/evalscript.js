//VERSION=3
function setup() {
    return {
        input: ["VV", "VH"],
        output: [{
            id: "_20201105",
            bands: 2,
            sampleType: "FLOAT32"
        }, {
            id: "_20201106",
            bands: 2,
            sampleType: "FLOAT32",
        }],
        mosaicking: "ORBIT"
    };
}

function evaluatePixel(samples, scenes) {
    return {
        _20201105: bandValues(samples, scenes, 0),
        _20201106: bandValues(samples, scenes, 1)
    };
}

function bandValues(samples, scenes, sceneIdx) {
    function indexOf(sceneIdx) {
        return scenes.findIndex(scene => scene.idx === sceneIdx)
    }

    let sampleIndex = indexOf(sceneIdx)
    return sampleIndex >= 0 ? [samples[sampleIndex].VV, samples[sampleIndex].VH] : [0, 0]
}
