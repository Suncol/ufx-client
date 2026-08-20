#include "print_listener.h"
#include "ufx/session.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

static void SleepSec(int sec) {
#ifdef _WIN32
    Sleep(sec * 1000);
#else
    sleep(static_cast<unsigned int>(sec));
#endif
}

int main(int argc, char** argv) {
    ufx::SessionConfig cfg;
    cfg.t2sdk_ini = "t2sdk.ini";
    cfg.subscriber_ini = "subscriber.ini";
    cfg.operator_no = "1000";
    cfg.password = "0";
    cfg.authorization_id = "1";
    cfg.account_code = "";
    cfg.combi_no = "";

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--op") == 0 && i + 1 < argc) {
            cfg.operator_no = argv[++i];
        } else if (std::strcmp(argv[i], "--pwd") == 0 && i + 1 < argc) {
            cfg.password = argv[++i];
        } else if (std::strcmp(argv[i], "--account") == 0 && i + 1 < argc) {
            cfg.account_code = argv[++i];
        } else if (std::strcmp(argv[i], "--asset") == 0 && i + 1 < argc) {
            cfg.asset_no = argv[++i];
        } else if (std::strcmp(argv[i], "--combi") == 0 && i + 1 < argc) {
            cfg.combi_no = argv[++i];
        } else if (std::strcmp(argv[i], "--auth") == 0 && i + 1 < argc) {
            cfg.authorization_id = argv[++i];
        } else if (std::strcmp(argv[i], "--t2") == 0 && i + 1 < argc) {
            cfg.t2sdk_ini = argv[++i];
        } else if (std::strcmp(argv[i], "--sub") == 0 && i + 1 < argc) {
            cfg.subscriber_ini = argv[++i];
        }
    }

    if (cfg.asset_no.empty() == cfg.combi_no.empty()) {
        std::fprintf(stderr,
                     "usage: ufx_demo --op 1000 --pwd 0 [--account ACCOUNT] "
                     "(--asset ASSET | --combi COMBI) "
                     "[--auth ID] [--t2 t2sdk.ini] [--sub subscriber.ini]\n");
        return 1;
    }

    const std::shared_ptr<ufx::PrintListener> listener(new ufx::PrintListener());
    ufx::Session session(cfg);
    session.SetListener(listener);
    const int rc = session.Start();
    if (rc != 0) {
        std::fprintf(stderr, "start failed: %d\n", rc);
        return 2;
    }

    std::printf("running, token=%s. Ctrl+C to stop after 8 hours or kill process.\n",
                session.UserToken().c_str());
    for (int i = 0; i < 8 * 60 && true; ++i) {
        SleepSec(60);
    }
    session.Stop();
    return 0;
}
