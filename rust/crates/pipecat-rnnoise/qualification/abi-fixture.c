/* Synthetic ABI failure fixtures, not an RNNoise implementation. BSD-2-Clause. */
#include <math.h>
#include <stdlib.h>
#ifndef FRAME_SIZE
#define FRAME_SIZE 480
#endif
int rnnoise_get_frame_size(void) { return FRAME_SIZE; }
void *rnnoise_create(void *model) {
    (void)model;
#ifdef NULL_STATE
    return NULL;
#else
    return malloc(1);
#endif
}
#ifndef MISSING_DESTROY
void rnnoise_destroy(void *state) { free(state); }
#endif
float rnnoise_process_frame(void *state, float *out, const float *in) {
    (void)state;
    for (int i = 0; i < 480; ++i) out[i] = in[i];
#ifdef NONFINITE_OUTPUT
    out[0] = NAN;
#endif
    return 0.5f;
}
