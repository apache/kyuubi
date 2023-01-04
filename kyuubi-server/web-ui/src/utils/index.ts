function secondTransfer(val: number) {
  let theTime = val
  let middle = 0
  let hour = 0

  if (theTime >= 60) {
    middle = Math.floor(theTime / 60)
    theTime = Math.floor(theTime % 60)
    if (middle >= 60) {
      hour = Math.floor(middle / 60)
      middle = Math.floor(middle % 60)
    }
  }
  let result = String(Math.floor(theTime))
  if (middle > 0) {
    result = Math.floor(middle) + ':' + result
  }
  if (hour > 0) {
    result = Math.floor(hour) + ':' + result
  }
  return result
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
