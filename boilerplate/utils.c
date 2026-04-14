#include "utils.h"
#include <stdio.h>
#include <stdlib.h>

void die_with_error(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}
