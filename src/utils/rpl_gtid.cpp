//
// Created by Coonger on 2024/10/20.
//
#include "utils/rpl_gtid.h"
#include "utils/little_endian.h"
#include <inttypes.h>  // PRId64

//*******************parse util *************************
void skip_whitespace(const char *s)
{
  while (s != nullptr && *s != '\0' && std::isspace(*s)) {
    ++s;
  }
}

rpl_gno parse_gno(const char **s)
{
  char     *endp;
  long long ret = strtoll(*s, &endp, 0);
  if (ret < 0 || ret >= GNO_END) {
    return -1;
  }
  *s = endp;
  return static_cast<rpl_gno>(ret);
}

char *longlong10_to_str(int64_t value, char *buffer, int radix)
{
  int64_t absValue = std::abs(value);
  int     index    = 0;

  do {
    int digit       = absValue % radix;
    buffer[index++] = (digit < 10) ? ('0' + digit) : ('a' + digit - 10);
    absValue /= radix;
  } while (absValue > 0);

  if (value < 0) {
    buffer[index++] = '-';
  }

  buffer[index] = '\0';
  std::reverse(buffer, buffer + index);

  return buffer;
}

int format_gno(char *s, rpl_gno gno) { return static_cast<int>(longlong10_to_str(gno, s, 10) - s); }

/**************************************************************************
        Gtid methods
**************************************************************************/

bool Gtid::is_valid(const char *text)
{
  const char *s = text;
  skip_whitespace(s);
  if (!rpl_sid::is_valid(s, binary_log::Uuid::TEXT_LENGTH)) {
    return false;
  }
  s += binary_log::Uuid::TEXT_LENGTH;
  skip_whitespace(s);
  if (*s != ':') {
    return false;
  }
  s++;
  skip_whitespace(s);
  if (parse_gno(&s) <= 0) {
    return false;
  }
  skip_whitespace(s);
  if (*s != 0) {
    return false;
  }
  return true;
}

int Gtid::to_string(const rpl_sid &sid, char *buf) const
{
  char *s = buf + sid.to_string(buf);
  *s      = ':';
  s++;
  s += format_gno(s, gno_);
  return (int)(s - buf);
}

int Gtid::to_string(const Sid_map *sid_map, char *buf) const
{
  int ret;
  if (sid_map != nullptr) {
    const rpl_sid &sid = sid_map->sidno_to_sid(sidno_);
    ret                = to_string(sid, buf);
  } else {
    ret = sprintf(buf, "%d:%" PRId64, sidno_, gno_);
  }
  return ret;
}

enum_return_status Gtid::parse(Sid_map *sid_map, const char *text)
{
  rpl_sid     sid{};
  const char *s = text;

  skip_whitespace(s);

  // parse sid
  if (sid.parse(s, binary_log::Uuid::TEXT_LENGTH) == 0) {
    rpl_sidno sidno_var = sid_map->add_sid(sid);
    if (sidno_var <= 0) {
      return RETURN_STATUS_REPORTED_ERROR;
    }
    s += binary_log::Uuid::TEXT_LENGTH;

    skip_whitespace(s);

    // parse colon
    if (*s == ':') {
      s++;

      skip_whitespace(s);

      // parse gno
      rpl_gno gno_var = parse_gno(&s);
      if (gno_var > 0) {
        skip_whitespace(s);
        if (*s == '\0') {
          sidno_ = sidno_var;
          gno_   = gno_var;
          return RETURN_STATUS_OK;
        }
      }
      return RETURN_STATUS_REPORTED_ERROR;
    }
  }
  // never reached
  return RETURN_STATUS_UNREPORTED_ERROR;
}

/**************************************************************************
        Gtid_specification methods
**************************************************************************/

bool Gtid_specification::is_valid(const char *text)
{
  // AUTOMATIC, ANONYMOUS, always return true
  return true;
}

enum_return_status Gtid_specification::parse(Sid_map *sid_map, const char *text)
{
  // TODO
  type_        = ANONYMOUS_GTID;
  gtid_.sidno_ = 0;
  gtid_.gno_   = 0;
  return RETURN_STATUS_OK;
}

int Gtid_specification::to_string(const rpl_sid *sid, char *buf) const
{
  switch (type_) {
    case AUTOMATIC_GTID: strcpy(buf, "AUTOMATIC"); return 9;
    case NOT_YET_DETERMINED_GTID: strcpy(buf, "NOT_YET_DETERMINED"); return 18;
    case ANONYMOUS_GTID: strcpy(buf, "ANONYMOUS"); return 9;
    case UNDEFINED_GTID:
    case ASSIGNED_GTID: return gtid_.to_string(*sid, buf);
    case PRE_GENERATE_GTID: strcpy(buf, "PRE_GENERATE_GTID"); return 17;
  }
  assert(0);
  return 0;
}

int Gtid_specification::to_string(const Sid_map *sid_map, char *buf) const
{
  return to_string(
      type_ == ASSIGNED_GTID || type_ == UNDEFINED_GTID ? &sid_map->sidno_to_sid(gtid_.sidno_) : nullptr, buf);
}

rpl_sidno Sid_map::add_sid(const rpl_sid &sid)
{
  rpl_sidno sidno;
  auto      it = sid_to_sidno_map_.find(sid);
  if (it != sid_to_sidno_map_.end()) {
    return it->second->sidno_;
  } else {
    sidno = get_max_sidno() + 1;
    if (add_node(sidno, sid) != RETURN_STATUS_OK) {
      sidno = -1;
    }
  }

  return sidno;
}

enum_return_status Sid_map::add_node(rpl_sidno sidno, const rpl_sid &sid)
{
  Node *node   = new Node();
  node->sidno_ = sidno;
  node->sid_   = sid;

  sidno_to_sid_map_.emplace_back(node);
  sid_to_sidno_map_.emplace(sid, std::move(node));

  return RETURN_STATUS_OK;
}

/**************************************************************************
        Gtid_set methods
**************************************************************************/

size_t Gtid_set::get_encoded_length() const
{
  size_t ret = 8;
  return ret;
}

void Gtid_set::encode(unsigned char *buf) const
{
  // make place for number of sids
  uint64_t       n_sids   = 0;
  unsigned char *n_sids_p = buf;
  buf += 8;
  // store number of sids
  int8store(n_sids_p, n_sids);
  assert(buf - n_sids_p == (int)get_encoded_length());
}
