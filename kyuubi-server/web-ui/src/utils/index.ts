function secondTransfer(val: number) {
  const h = Math.floor(val / 3600)
  const min = Math.floor((val - 3600 * h) / 60)
  const sec = Math.round(val - 3600 * h - 60 * min)
  return h === 0
    ? min == 0
      ? `${sec}sec`
      : sec === 0
      ? `${min}min`
      : `${min}min${sec}sec`
    : sec === 0
    ? min !== 0
      ? `${h}hour${min}min`
      : `${h}hour`
    : `${h}hour${min}min${sec}sec`
}

function unitTransfer(diShu: number, unitArr: any[], mi: number, left: number) {
  const duiShu = Math.floor(Math.log(diShu) / Math.log(mi))
  const index = duiShu < unitArr.length ? duiShu : unitArr.length - 1
  return `${Number((diShu / Math.pow(mi, index)).toFixed(left))}${
    unitArr[index]
  }`
}

function byteTransfer(
  diShu: number,
  unitArr = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB'],
  mi = 1024,
  left = 2
) {
  return diShu === 0 ? 0 : unitTransfer(diShu, unitArr, mi, left)
}

export default byteTransfer

export { secondTransfer, byteTransfer }
