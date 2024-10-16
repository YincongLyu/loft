//
// Created by Coonger on 2024/10/17.
//
#include "fde_generated.h"
#include "control_events.h"

#include <gtest/gtest.h>
#include <fstream>


TEST(EVENT_FORMAT_TEST, FORMAT_DESCRIPTION_EVENT) {
    Format_description_event fde(4, "8.0.26");

    std::ofstream file("fde.bin", std::ios::binary);

    fde.write(file);

    file.close();
}