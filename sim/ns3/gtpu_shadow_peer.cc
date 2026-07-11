// SPDX-License-Identifier: GPL-2.0-only

#include "gtpu_shadow_peer.h"

#include <type_traits>

namespace ns3
{

static_assert(std::is_base_of_v<Object, GtpuShadowPeer>);

} // namespace ns3
