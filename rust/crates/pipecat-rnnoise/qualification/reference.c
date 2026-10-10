/* Synthetic fixture and direct public-C-ABI oracle. BSD-2-Clause.
 * This program is a test harness, not an RNNoise implementation.
 * No human recordings, datasets, or provider calls are used. */
#include <dlfcn.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef void *(*create_fn)(void *);
typedef void (*destroy_fn)(void *);
typedef float (*process_fn)(void *, float *, const float *);
typedef int (*size_fn)(void);
static void fail(const char *text) { fprintf(stderr, "%s\n", text); exit(1); }
static void write_sample(FILE *file, int16_t value) {
    uint16_t bits = (uint16_t)value;
    if (fputc(bits & 255, file) == EOF || fputc(bits >> 8, file) == EOF) fail("write failed");
}
static int16_t to_pcm(float sample) {
    if (sample > 32767) sample = 32767;
    if (sample < -32768) sample = -32768;
    return (int16_t)sample;
}
static FILE *output(const char *directory, const char *name) {
    char path[4096];
    if (snprintf(path, sizeof(path), "%s/%s", directory, name) >= (int)sizeof(path)) fail("path too long");
    FILE *file = fopen(path, "wb"); if (!file) fail("cannot open output"); return file;
}
int main(int argc, char **argv) {
    if (argc != 3) fail("usage: reference absolute-library-path output-directory");
    void *library = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL); if (!library) fail(dlerror());
    create_fn create; destroy_fn destroy; process_fn process; size_fn frame_size;
    void *symbol = dlsym(library, "rnnoise_create"); memcpy(&create, &symbol, sizeof(create));
    symbol = dlsym(library, "rnnoise_destroy"); memcpy(&destroy, &symbol, sizeof(destroy));
    symbol = dlsym(library, "rnnoise_process_frame"); memcpy(&process, &symbol, sizeof(process));
    symbol = dlsym(library, "rnnoise_get_frame_size"); memcpy(&frame_size, &symbol, sizeof(frame_size));
    if (!create || !destroy || !process || !frame_size || frame_size() != 480) fail("wrong ABI");
    void *state = create(NULL); if (!state) fail("null state");
    FILE *input = output(argv[2], "synthetic-noisy.pcm");
    FILE *clean = output(argv[2], "synthetic-clean.pcm");
    FILE *result = output(argv[2], "reference-denoised.pcm");
    uint32_t random = 0x12345678;
    for (int frame = 0; frame < 300; ++frame) {
        float in[480], out[480];
        for (int j = 0; j < 480; ++j) {
            int index = frame * 480 + j;
            double t = index / 48000.0;
            double envelope = (t >= 0.5 && t < 2.0) ? fmax(0, sin(2 * M_PI * 2 * (t - 0.5))) : 0;
            double speech = envelope * (9000 * sin(2*M_PI*200*t) + 5000 * sin(2*M_PI*400*t) + 3000 * sin(2*M_PI*600*t));
            random ^= random << 13; random ^= random >> 17; random ^= random << 5;
            double noise = ((double)(random & 65535) / 32767.5 - 1) * 5000;
            int16_t noisy = to_pcm((float)(speech + noise));
            write_sample(input, noisy); write_sample(clean, to_pcm((float)speech));
            in[j] = noisy;
        }
        float probability = process(state, out, in);
        if (!isfinite(probability) || probability < 0 || probability > 1) fail("invalid probability");
        for (int j = 0; j < 480; ++j) {
            if (!isfinite(out[j])) fail("nonfinite native output");
            write_sample(result, to_pcm(out[j]));
        }
    }
    if (fclose(input) || fclose(clean) || fclose(result)) fail("close failed");
    destroy(state); dlclose(library);
    return 0;
}
