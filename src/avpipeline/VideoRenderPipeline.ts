/*
 * libmedia VideoRenderPipeline
 *
 * 版权所有 (C) 2024 赵高兴
 * Copyright (C) 2024 Gaoxing Zhao
 *
 * 此文件是 libmedia 的一部分
 * This file is part of libmedia.
 * 
 * libmedia 是自由软件；您可以根据 GNU Lesser General Public License（GNU LGPL）3.1
 * 或任何其更新的版本条款重新分发或修改它
 * libmedia is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.1 of the License, or (at your option) any later version.
 * 
 * libmedia 希望能够为您提供帮助，但不提供任何明示或暗示的担保，包括但不限于适销性或特定用途的保证
 * 您应自行承担使用 libmedia 的风险，并且需要遵守 GNU Lesser General Public License 中的条款和条件。
 * libmedia is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 */

import Pipeline, { TaskOptions } from './Pipeline'
import * as errorType from 'avutil/error'
import IPCPort from 'common/network/IPCPort'
import AVFrame, { AVFrameRef } from 'avutil/struct/avframe'
import * as is from 'common/util/is'
import * as array from 'common/util/array'
import List from 'cheap/std/collection/List'
import { Mutex } from 'cheap/thread/mutex'
import AVFramePoolImpl from 'avutil/implement/AVFramePoolImpl'
import ImageRender from 'avrender/image/ImageRender'
import support from 'common/util/support'
import { RenderMode } from 'avrender/image/ImageRender'
import LoopTask from 'common/timer/LoopTask'
import getTimestamp from 'common/function/getTimestamp'
import { avRescaleQ, avRescaleQ2 } from 'avutil/util/rational'
import { AV_MILLI_TIME_BASE_Q, AV_TIME_BASE_Q, NOPTS_VALUE_BIGINT } from 'avutil/constant'
import { NOTIFY, RpcMessage } from 'common/network/IPCPort'
import browser from 'common/util/browser'

import * as logger from 'common/util/logger'
import * as bigint from 'common/util/bigint'

import WebGPURender, { WebGPURenderOptions } from 'avrender/image/WebGPURender'
import WebGLRender, { WebGLRenderOptions } from 'avrender/image/WebGLRender'
import CanvasImageRender from 'avrender/image/Canvas2dRender'
import WebGPUExternalRender from 'avrender/image/WebGPUExternalRender'
import WebGLDefault8Render from 'avrender/image/WebGLDefault8Render'
import WebGLDefault16Render from 'avrender/image/WebGLDefault16Render'
import WebGPUDefault8Render from 'avrender/image/WebGPUDefault8Render'
import WebGPUDefault16Render from 'avrender/image/WebGPUDefault16Render'
import WritableStreamRender from 'avrender/image/WritableStreamRender'
import isWorker from 'common/function/isWorker'
import nextTick from 'common/function/nextTick'
import isPointer from 'cheap/std/function/isPointer'
import os from 'common/util/os'
import MasterTimer from 'common/timer/MasterTimer'

type WebGPURenderFactory = {
  new(canvas: HTMLCanvasElement | OffscreenCanvas, options: WebGPURenderOptions): WebGPURender,
  isSupport: (frame: pointer<AVFrame> | VideoFrame | ImageBitmap) => boolean
}
type WebGLRenderFactory = {
  new(canvas: HTMLCanvasElement | OffscreenCanvas, options: WebGLRenderOptions): WebGLRender,
  isSupport: (frame: pointer<AVFrame> | VideoFrame | ImageBitmap) => boolean
}

const WebGPURenderList: WebGPURenderFactory[] = defined(ENABLE_WEBGPU) ? [
  WebGPUDefault8Render
] : []
if (defined(ENABLE_WEBGPU) && defined(ENABLE_RENDER_16)) {
  WebGPURenderList.push(WebGPUDefault16Render)
}

const WebGLRenderList: WebGLRenderFactory[] = [
  WebGLDefault8Render
]
if (defined(ENABLE_RENDER_16)) {
  WebGLRenderList.push(WebGLDefault16Render)
}

enum AdjustStatus {
  None,
  Accelerate,
  Decelerate
}

let disableWebGPU = false

export interface VideoRenderTaskOptions extends TaskOptions {
  canvas: HTMLCanvasElement | OffscreenCanvas | WritableStream<VideoFrame>
  renderMode: RenderMode
  renderRotate: double
  flipHorizontal: boolean
  flipVertical: boolean
  viewportWidth: int32
  viewportHeight: int32
  devicePixelRatio: double
  enableWebGPU: boolean
  startPTS: int64
  avframeList: pointer<List<pointer<AVFrameRef>>>
  avframeListMutex: pointer<Mutex>
  enableJitterBuffer: boolean
  isLive: boolean
}

type SelfTask = VideoRenderTaskOptions & {
  leftIPCPort: IPCPort
  controlIPCPort: IPCPort

  currentPTS: int64
  lastAdjustTimestamp: int64
  lastMasterPts: int64
  masterTimer: MasterTimer
  lastNotifyPTS: int64
  // playRate / 100 
  playRate: int64
  frontFrame: pointer<AVFrameRef> | VideoFrame
  backFrame: pointer<AVFrameRef> | VideoFrame
  renderFrame: pointer<AVFrameRef> | VideoFrame
  renderFrameCount: int64

  loop: LoopTask
  render: ImageRender
  renderRedyed: boolean
  renderRecreateCount: number

  adjust: AdjustStatus
  adjustDiff: int64

  firstRendered: boolean
  canvasUpdated: boolean
  renderCreating: boolean

  skipRender: boolean

  isSupport: (frame: pointer<AVFrame> | VideoFrame | ImageBitmap) => boolean

  frontBuffered: boolean
  ended: boolean

  seeking: boolean
  seekSync: () => void
  afterPullResolver: () => void
  pausing: boolean

  lastRenderTimestamp: number

  avframePool: AVFramePoolImpl

  // 历史帧相关
  historyFrames?: CircularFrameBuffer
  // 是否在帧导航模式
  inFrameNavigation: boolean
  // 是否正在从历史帧播放 
  playingFromHistory: boolean
}

const HHRPrimaries = ['bt2020', 'bt2100', 'st2048', 'p3-dcl', 'hlg']

function isHDR(primaries: string) {
  return array.has(HHRPrimaries, primaries)
}

/**
 * 循环帧缓冲区 - 管理历史帧并提供高效的前后导航
 */
class CircularFrameBuffer {
  private frames: (pointer<AVFrameRef> | VideoFrame)[] = []
  private capacity: number
  private currentIndex: number = -1
  private framePool: AVFramePoolImpl
  private enableCapture: boolean = true
  private inFrameNavigation: boolean = false

  constructor(capacity: number, framePool: AVFramePoolImpl) {
    this.capacity = capacity
    this.framePool = framePool
  }

  public setFrameNavigation(enable: boolean): void {
    this.inFrameNavigation = enable
    if (enable) {
      this.currentIndex = this.frames.length - 1
    }
    else {
      this.currentIndex = -1
    }
  }

  public getFrameNavigation(): boolean {
    return this.inFrameNavigation
  }

  /**
   * 设置是否捕获帧
   */
  public setEnableCapture(enable: boolean): void {
    this.enableCapture = enable
    if (!enable) {
      this.clear()
    }
  }

  /**
   * 添加一帧到历史缓冲区
   */
  public push(frame: pointer<AVFrameRef> | VideoFrame): void {
    if (!this.enableCapture) {
      return
    }

    // 创建帧副本
    let frameCopy: pointer<AVFrameRef> | VideoFrame
    if (isPointer(frame)) {
      frameCopy = this.framePool.alloc()
      this.framePool.clone(frameCopy, frame)
    }
    else {
      frameCopy = frame.clone()
    }

    // 管理容量
    if (this.frames.length >= this.capacity) {
      // 移除最旧的帧
      const oldFrame = this.frames.shift()
      this.releaseFrame(oldFrame)
      // 调整当前索引
      if (this.currentIndex > 0) {
        this.currentIndex--
      }
    }

    // 添加新帧
    this.frames.push(frameCopy)
    this.currentIndex = this.frames.length - 1
  }

  /**
   * 获取上一帧
   */
  public prev(): pointer<AVFrameRef> | VideoFrame | null {
    if (this.currentIndex > 0) {
      this.currentIndex--
      console.log('获取上一帧', this.currentIndex, this.frames.length)
      return this.cloneFrame(this.frames[this.currentIndex])
    }
    return null
  }

  /**
   * 获取下一帧
   */
  public next(): pointer<AVFrameRef> | VideoFrame | null {
    if (this.currentIndex < this.frames.length - 1) {
      this.currentIndex++
      console.log('获取下一帧', this.currentIndex, this.frames.length)
      return this.cloneFrame(this.frames[this.currentIndex])
    }
    return null
  }

  /**
   * 获取当前帧
   */
  public current(): pointer<AVFrameRef> | VideoFrame | null {
    if (this.currentIndex >= 0 && this.currentIndex < this.frames.length) {
      return this.cloneFrame(this.frames[this.currentIndex])
    }
    return null
  }

  /**
   * 清空缓冲区
   */
  public clear(): void {
    this.frames.forEach((frame) => this.releaseFrame(frame))
    this.frames = []
    this.currentIndex = -1
  }

  /**
   * 获取历史帧数量
   */
  public size(): number {
    return this.frames.length
  }

  /**
   * 获取当前位置
   */
  public position(): number {
    return this.currentIndex
  }

  /**
   * 释放帧资源
   */
  private releaseFrame(frame: pointer<AVFrameRef> | VideoFrame): void {
    if (isPointer(frame)) {
      this.framePool.release(frame)
    }
    else {
      frame.close()
    }
  }

  private cloneFrame(frame: pointer<AVFrameRef> | VideoFrame): pointer<AVFrameRef> | VideoFrame {
    if (isPointer(frame)) {
      const clonedFrame = this.framePool.alloc()
      this.framePool.clone(clonedFrame, frame)
      return clonedFrame
    }
    else {
      return frame.clone()
    }
  }
}

export default class VideoRenderPipeline extends Pipeline {

  declare tasks: Map<string, SelfTask>

  constructor() {
    super()
  }

  private async createTask(options: VideoRenderTaskOptions): Promise<number> {
    const leftIPCPort = new IPCPort(options.leftPort)
    const controlIPCPort = new IPCPort(options.controlPort)

    const task: SelfTask = {
      ...options,
      leftIPCPort,
      controlIPCPort,
      render: null,
      frontFrame: null,
      backFrame: null,
      renderFrame: null,
      renderFrameCount: 0n,

      loop: null,
      renderRedyed: false,
      currentPTS: NOPTS_VALUE_BIGINT,
      adjust: AdjustStatus.None,
      adjustDiff: 0n,
      lastAdjustTimestamp: 0n,
      lastMasterPts: NOPTS_VALUE_BIGINT,
      masterTimer: new MasterTimer(),
      lastNotifyPTS: 0n,
      playRate: 100n,

      firstRendered: false,
      canvasUpdated: false,
      renderCreating: false,
      renderRecreateCount: 0,

      skipRender: false,

      isSupport: () => false,

      frontBuffered: false,
      ended: false,
      seeking: false,
      seekSync: null,
      afterPullResolver: null,
      pausing: false,

      lastRenderTimestamp: 0,
      avframePool: new AVFramePoolImpl(accessof(options.avframeList), options.avframeListMutex),
      inFrameNavigation: false,
      playingFromHistory: false
    }
    task.historyFrames = new CircularFrameBuffer(10, task.avframePool)

    controlIPCPort.on(NOTIFY, async (request: RpcMessage) => {
      switch (request.method) {
        case 'syncPts': {
          const targetMaster = request.params.pts
          const currentMaster = task.masterTimer.getMasterTime()
          const diff = Math.abs(Number(targetMaster - currentMaster))
          if (diff > 100 && task.currentPTS > 0n) {
            if (targetMaster > currentMaster) {
              task.adjust = AdjustStatus.Accelerate
              logger.debug(`video render sync master time accelerate, targetMaster: ${targetMaster}, currentMaster: ${currentMaster}, diff: ${diff}, taskId: ${task.taskId}`)
            }
            else {
              task.adjust = AdjustStatus.Decelerate
              logger.debug(`video render sync master time decelerate, targetMaster: ${targetMaster}, currentMaster: ${currentMaster}, diff: ${diff} taskId: ${task.taskId}`)
            }
            task.adjustDiff = static_cast<int64>(diff)
          }
          break
        }
        case 'skipRender': {
          task.skipRender = request.params.skipRender
          break
        }
      }
    })

    this.tasks.set(options.taskId, task)
    return 0
  }

  private swap(task: SelfTask) {
    if (task.seeking) {
      return
    }

    // 如果正在从历史帧播放，让 LoopTask 处理帧转换
    if (task.playingFromHistory) {
      return
    }

    // 在播放过程中捕获帧 ( 不在帧导航模式下）
    if (task.backFrame && !task.seeking && !task.inFrameNavigation && task.historyFrames) {
      console.log('捕获帧到历史缓冲区')
      task.historyFrames.push(task.backFrame)
    }

    if (task.backFrame) {
      if (!isPointer(task.backFrame)) {
        task.backFrame.close()
      }
      else {
        task.avframePool.release(task.backFrame)
      }
    }

    task.backFrame = null
    if (task.frontBuffered) {
      task.backFrame = task.frontFrame
      task.frontFrame = null
      task.stats.videoNextTime = isPointer(task.backFrame)
        ? avRescaleQ2(
          task.backFrame.pts,
          addressof(task.backFrame.timeBase),
          AV_MILLI_TIME_BASE_Q
        )
        : avRescaleQ(
          static_cast<int64>(task.backFrame.timestamp as uint32),
          AV_TIME_BASE_Q,
          AV_MILLI_TIME_BASE_Q
        )
    }
    else {
      return false
    }
    if (task.ended) {
      return
    }
    task.frontBuffered = false

    task.leftIPCPort.request<pointer<AVFrameRef> | VideoFrame>('pull').then((frame) => {
      if (task.afterPullResolver) {
        task.afterPullResolver()
      }
      if (is.number(frame) && frame < 0) {
        task.ended = true
        task.frontFrame = null
        return
      }
      assert(!isPointer(frame) || frame.data[0], 'got empty video frame')
      task.frontFrame = frame
      task.frontBuffered = true
      if (task.seekSync) {
        task.seekSync()
        task.seekSync = null
        return
      }
      if (!task.backFrame) {
        this.swap(task)
      }
    })
    return true
  }

  private fakeSyncPts(task: SelfTask) {
    const pts = task.masterTimer.getMasterTime()
    if (pts - task.lastNotifyPTS >= 1000n) {
      task.lastNotifyPTS = pts
      task.stats.videoCurrentTime = pts
      task.controlIPCPort.notify('syncPts', {
        pts
      })
    }
  }

  private async createRender(task: SelfTask, frame: pointer<AVFrameRef> | VideoFrame, fallback: boolean = false) {
    if (task.renderCreating) {
      return
    }

    task.renderCreating = true
    task.renderRedyed = false

    if (task.render) {
      task.render.destroy()
    }

    if (typeof WritableStream === 'function' && task.canvas instanceof WritableStream) {
      task.render = new WritableStreamRender(task.canvas)
      task.isSupport = WritableStreamRender.isSupport
    }
    else {
      if (!isPointer(frame)) {
        if (defined(ENABLE_WEBGPU)
          && task.enableWebGPU
          && support.webgpu
          && !disableWebGPU
          && (
            // chrome116+ webgpu 可以导入 VideoFrame 作为纹理
            (browser.chrome || browser.newEdge) && browser.checkVersion(browser.majorVersion, '116', true)
            || browser.safari && browser.checkVersion(browser.majorVersion, '17.4', true)
            || os.ios && browser.checkVersion(os.version, '17.4', true)
            || browser.firefox && browser.checkVersion(browser.majorVersion, '129', true)
          )
          && !isHDR(frame.colorSpace.primaries)
        ) {
          // WebGPUExternalRender 性能最优
          task.render = new WebGPUExternalRender(task.canvas as OffscreenCanvas, {
            devicePixelRatio: task.devicePixelRatio,
            renderMode: task.renderMode,
            onRenderContextLost: () => {
              task.canvasUpdated = false
              task.renderRedyed = false
              logger.warn('render context lost')
              task.controlIPCPort.notify('updateCanvas')
            }
          })
          task.isSupport = WebGPUExternalRender.isSupport
        }
        else {
          // CanvasImageRender 支持 hdr 视频渲染
          task.render = new CanvasImageRender(task.canvas as OffscreenCanvas, {
            devicePixelRatio: task.devicePixelRatio,
            renderMode: task.renderMode,
            // @ts-ignore
            colorSpace: frame.colorSpace.transfer === 'pq'
              ? 'rec2100-pq'
              // @ts-ignore
              : (frame.colorSpace.transfer === 'hlg'
                ? 'rec2100-hlg'
                : undefined
              )
          })
          task.isSupport = CanvasImageRender.isSupport
        }
      }
      else {
        // 优先使用 webgpu，webgpu 性能优于 webgl
        if (defined(ENABLE_WEBGPU)
          && task.enableWebGPU
          && support.webgpu
          && !disableWebGPU
        ) {
          array.each(WebGPURenderList, (RenderFactory) => {
            if (RenderFactory.isSupport(frame)) {
              task.render = new RenderFactory(task.canvas as OffscreenCanvas, {
                devicePixelRatio: task.devicePixelRatio,
                renderMode: task.renderMode,
                onRenderContextLost: () => {
                  task.canvasUpdated = false
                  task.renderRedyed = false
                  logger.warn('render context lost')
                  task.controlIPCPort.notify('updateCanvas')
                }
              })
              task.isSupport = RenderFactory.isSupport
              return false
            }
          })
        }
        if (!task.render) {
          array.each(WebGLRenderList, (RenderFactory) => {
            if (RenderFactory.isSupport(frame)) {
              task.render = new RenderFactory(task.canvas as OffscreenCanvas, {
                devicePixelRatio: task.devicePixelRatio,
                renderMode: task.renderMode,
                onRenderContextLost: () => {
                  task.canvasUpdated = false
                  task.renderRedyed = false
                  logger.warn('render context lost')
                  task.controlIPCPort.notify('updateCanvas')
                }
              })
              task.isSupport = RenderFactory.isSupport
              return false
            }
          })
        }
      }
    }
    if (!task.render) {
      logger.error(`not found format ${frame.format} render`)
      task.renderCreating = false
      return
    }

    try {
      await task.render.init()
      task.render.viewport(task.viewportWidth, task.viewportHeight)
      task.render.setRotate(task.renderRotate ?? 0, false)
      task.render.enableHorizontalFlip(task.flipHorizontal ?? false)
      task.render.enableVerticalFlip(task.flipVertical ?? false)
      task.render.render(frame)
      task.render.clear()
    }
    catch (error) {
      if (defined(ENABLE_WEBGPU)) {
        if (task.render instanceof WebGPURender && !fallback) {
          disableWebGPU = true
          task.renderCreating = false
          logger.warn('not support webgpu render, try to fallback to webgl render')
          return this.createRender(task, frame, true)
        }
        else if (fallback) {
          task.renderCreating = false
          logger.warn('canvas context lost after fallback, wait for recreate canvas')
          if (task.renderRecreateCount < 3) {
            task.renderRecreateCount++
            task.controlIPCPort.notify('updateCanvas')
            return
          }
          throw error
        }
      }
      else {
        throw error
      }
    }
    task.renderRedyed = true
    task.renderCreating = false
    task.renderRecreateCount = 0
  }

  public async play(taskId: string) {
    const task = this.tasks.get(taskId)
    if (task) {
      if (task.loop) {
        logger.fatal('task has already call play')
      }

      task.backFrame = await task.leftIPCPort.request<pointer<AVFrameRef> | VideoFrame>('pull')

      if (is.number(task.backFrame) && task.backFrame < 0) {
        task.backFrame = nullptr
        logger.info(`video render ended, taskId: ${task.taskId}`)
        task.controlIPCPort.notify('ended')
        return
      }

      task.frontFrame = await task.leftIPCPort.request<pointer<AVFrameRef> | VideoFrame>('pull')

      task.frontBuffered = true
      task.ended = false

      if (is.number(task.frontFrame) && task.frontFrame < 0) {
        task.ended = true
        task.frontBuffered = false
        task.frontFrame = nullptr
      }

      await this.createRender(task, task.backFrame)

      task.currentPTS = isPointer(task.backFrame)
        ? avRescaleQ2(
          task.backFrame.pts,
          addressof(task.backFrame.timeBase),
          AV_MILLI_TIME_BASE_Q
        )
        : avRescaleQ(
          static_cast<int64>(task.backFrame.timestamp as uint32),
          AV_TIME_BASE_Q,
          AV_MILLI_TIME_BASE_Q
        )

      logger.debug(`got first video frame, pts: ${!isPointer(task.backFrame)
        ? static_cast<int64>(task.backFrame.timestamp as uint32)
        : task.backFrame.pts
      }(${task.currentPTS}ms), taskId: ${task.taskId}`)

      task.masterTimer.setMasterTime(task.lastMasterPts === NOPTS_VALUE_BIGINT ? task.startPTS : task.lastMasterPts)
      task.lastMasterPts = NOPTS_VALUE_BIGINT
      task.stats.videoNextTime = task.currentPTS

      const inWorker = isWorker()

      const interval = (inWorker && support.shareArrayBuffer) ? 0 : 10

      task.loop = new LoopTask(() => {
        // 如果正在从历史帧播放
        if (task.playingFromHistory && task.historyFrames) {
          // 获取下一个历史帧
          const nextFrame = task.historyFrames.next()
          if (nextFrame) {
            console.log('获取历史帧', task.historyFrames.position(), task.historyFrames.size())
            // 释放当前帧 ( 注意，这里可能需要按照你的内存管理策略调整）
            if (task.backFrame) {
              if (isPointer(task.backFrame)) {
                task.avframePool.release(task.backFrame)
              }
              else {
                task.backFrame.close()
              }
            }

            // 设置新的当前帧
            task.backFrame = nextFrame

            // 计算 PTS - 复用您原有的 PTS 计算代码
            const pts = isPointer(task.backFrame)
              ? avRescaleQ2(
                task.backFrame.pts,
                addressof(task.backFrame.timeBase),
                AV_MILLI_TIME_BASE_Q
              )
              : avRescaleQ(
                static_cast<int64>(task.backFrame.timestamp as uint32),
                AV_TIME_BASE_Q,
                AV_MILLI_TIME_BASE_Q
              )

            // 渲染帧
            if (task.renderRedyed && !task.skipRender) {
              task.render.render(task.backFrame)
              task.stats.videoFrameRenderCount++
              if (task.lastRenderTimestamp) {
                task.stats.videoFrameRenderIntervalMax = Math.max(
                  getTimestamp() - task.lastRenderTimestamp,
                  task.stats.videoFrameRenderIntervalMax
                )
              }
              task.lastRenderTimestamp = getTimestamp()
            }

            // 更新状态
            task.stats.videoCurrentTime = pts
            task.renderFrameCount++
            if (isPointer(task.backFrame)) {
              task.stats.width = task.backFrame.width
              task.stats.height = task.backFrame.height
            }
            else {
              task.stats.width = task.backFrame.displayWidth
              task.stats.height = task.backFrame.displayHeight
            }
            task.currentPTS = pts

            // 检查是否已到达最后一帧
            if (task.historyFrames.position() >= task.historyFrames.size() - 1) {
              // 退出历史帧播放模式，切换到正常播放
              task.playingFromHistory = false
              task.inFrameNavigation = false

              logger.debug(`完成历史帧播放，切换到正常播放模式，taskId: ${task.taskId}`)
            }

            return
          }
          else {
            // 没有更多历史帧，切换到正常播放
            task.playingFromHistory = false
            task.inFrameNavigation = false

            logger.debug(`没有更多历史帧，切换到正常播放模式，taskId: ${task.taskId}`)
          }
        }


        if (!task.backFrame) {
          if (!task.ended) {
            task.loop.emptyTask()
          }
          else {
            if (task.render && task.renderRedyed) {
              task.render.clear()
            }
            task.loop.stop()
            task.adjust = AdjustStatus.None
            logger.info(`video render ended, taskId: ${task.taskId}`)
            task.controlIPCPort.notify('ended')
          }
          return
        }

        if (!task.isSupport(task.backFrame)) {
          if (task.render) {
            if (!task.renderRedyed) {
              return
            }
            task.renderRedyed = false
            task.renderFrame = task.backFrame
            task.controlIPCPort.notify('updateCanvas')
            return
          }
          else {
            this.createRender(task, task.backFrame)
          }
        }

        const pts = isPointer(task.backFrame)
          ? avRescaleQ2(
            task.backFrame.pts,
            addressof(task.backFrame.timeBase),
            AV_MILLI_TIME_BASE_Q
          )
          : avRescaleQ(
            static_cast<int64>(task.backFrame.timestamp as uint32),
            AV_TIME_BASE_Q,
            AV_MILLI_TIME_BASE_Q
          )

        if (pts < task.currentPTS) {
          // 差值大于 5s 认为从头开始了
          if (task.currentPTS - pts > 5000n) {
            task.masterTimer.setMasterTime(pts)
          }
          else {
            logger.warn(`dropping frame with pts ${pts}, which is earlier then the last rendered frame(${task.currentPTS}), taskId: ${task.taskId}`)
            this.swap(task)
            return
          }
        }
        // 直播差值大于 4s 认为从某一处开始了
        else if ((pts - task.currentPTS > 4000n) && task.isLive) {
          task.masterTimer.setMasterTime(pts)
        }

        if (task.adjust === AdjustStatus.Accelerate) {
          if (task.adjustDiff <= 0) {
            task.adjust = AdjustStatus.None
            task.lastAdjustTimestamp = 0n
          }
          else {
            if (static_cast<int64>(getTimestamp()) - task.lastAdjustTimestamp >= 200n) {
              const add = bigint.min(task.adjustDiff, 100n)

              task.masterTimer.setMasterTime(task.masterTimer.getMasterTime() + add)
              task.adjustDiff -= add

              logger.debug(`video render accelerate master time add: ${add}, taskId: ${task.taskId}`)

              task.lastAdjustTimestamp = static_cast<int64>(getTimestamp())
            }
          }
        }
        else if (task.adjust === AdjustStatus.Decelerate) {
          if (task.adjustDiff <= 0) {
            task.adjust = AdjustStatus.None
            task.lastAdjustTimestamp = 0n
          }
          else {
            if (static_cast<int64>(getTimestamp()) - task.lastAdjustTimestamp >= 300n) {
              const sub = bigint.min(task.adjustDiff, 50n)

              task.masterTimer.setMasterTime(task.masterTimer.getMasterTime() - sub)
              task.adjustDiff -= sub

              logger.debug(`video render decelerate master time sub: ${sub}, taskId: ${task.taskId}`)

              task.lastAdjustTimestamp = static_cast<int64>(getTimestamp())
            }
          }
        }

        if (task.enableJitterBuffer) {
          let buffer = task.stats.videoPacketQueueLength / task.stats.videoEncodeFramerate * 1000
          if (buffer <= task.stats.jitterBuffer.min) {
            this.setPlayRate(task.taskId, 1)
          }
        }

        const diff = pts - task.masterTimer.getMasterTime()

        if (diff <= 0) {
          // 太晚的帧跳过渲染
          if (task.renderRedyed
            && !task.skipRender
            && (inWorker || (-diff < 100n) || (task.renderFrameCount & 0x01n))
          ) {
            task.render.render(task.backFrame)
            task.stats.videoFrameRenderCount++
            if (task.lastRenderTimestamp) {
              task.stats.videoFrameRenderIntervalMax = Math.max(
                getTimestamp() - task.lastRenderTimestamp,
                task.stats.videoFrameRenderIntervalMax
              )
            }
            task.lastRenderTimestamp = getTimestamp()
          }
          else {
            task.stats.videoFrameDropCount++
          }
          task.stats.videoCurrentTime = pts
          task.renderFrameCount++
          if (isPointer(task.backFrame)) {
            task.stats.width = task.backFrame.width
            task.stats.height = task.backFrame.height
          }
          else {
            task.stats.width = task.backFrame.displayWidth
            task.stats.height = task.backFrame.displayHeight
          }
          if (!task.firstRendered) {
            task.controlIPCPort.notify(task.canvasUpdated ? 'firstRenderedAfterUpdateCanvas' : 'firstRendered')
            task.firstRendered = true
            task.canvasUpdated = false
          }
          task.currentPTS = pts

          if (pts - task.lastNotifyPTS >= 1000n) {
            task.lastNotifyPTS = pts
            task.controlIPCPort.notify('syncPts', {
              pts
            })
          }
          this.swap(task)
          if (task.masterTimer.getMasterTime() - pts >= 2000n) {
            task.masterTimer.setMasterTime(pts)
          }
        }
        else {
          this.fakeSyncPts(task)
          task.loop.emptyTask()
        }
      }, 0, interval)

      task.loop.start()
    }
    else {
      logger.fatal('task not found')
    }
  }

  public async restart(taskId: string) {
    const task = this.tasks.get(taskId)
    if (task) {
      if (!task.loop) {
        logger.fatal('task has not played')
      }

      if (task.loop.isStarted()) {
        logger.fatal('task has already run')
      }

      if (task.backFrame) {
        if (!isPointer(task.backFrame)) {
          task.backFrame.close()
        }
        else {
          task.avframePool.release(task.backFrame)
        }
      }
      if (task.frontFrame) {
        if (!isPointer(task.frontFrame)) {
          task.frontFrame.close()
        }
        else {
          task.avframePool.release(task.frontFrame)
        }
      }

      task.backFrame = await task.leftIPCPort.request<pointer<AVFrameRef> | VideoFrame>('pull')
      task.frontFrame = await task.leftIPCPort.request<pointer<AVFrameRef> | VideoFrame>('pull')

      task.frontBuffered = true
      task.ended = false
      task.adjust = AdjustStatus.None
      task.lastNotifyPTS = NOPTS_VALUE_BIGINT
      task.firstRendered = false

      task.currentPTS = isPointer(task.backFrame)
        ? avRescaleQ2(
          task.backFrame.pts,
          addressof(task.backFrame.timeBase),
          AV_MILLI_TIME_BASE_Q
        )
        : avRescaleQ(
          static_cast<int64>(task.backFrame.timestamp as uint32),
          AV_TIME_BASE_Q,
          AV_MILLI_TIME_BASE_Q
        )

      task.masterTimer.setMasterTime(task.startPTS)

      task.loop.start()
    }
  }

  public async pause(taskId: string) {
    const task = this.tasks.get(taskId)
    if (task) {
      if (!task.loop) {
        logger.fatal('task has not played')
      }
      task.loop.stop()
      task.pausing = true
      task.lastMasterPts = task.masterTimer.getMasterTime()

      logger.info(`task paused, taskId: ${task.taskId}`)
    }
  }

  public async unpause(taskId: string) {
    const task = this.tasks.get(taskId)
    if (task) {
      if (task.seeking) {
        return
      }
      if (!task.loop) {
        logger.fatal('task has not played')
      }
      task.masterTimer.setMasterTime(task.lastMasterPts)
      task.lastMasterPts = NOPTS_VALUE_BIGINT

      if (task.inFrameNavigation && task.historyFrames && task.historyFrames.position() < task.historyFrames.size() - 1) {
        // 标记为从历史帧播放状态
        task.playingFromHistory = true
      }

      if (!task.seeking) {
        task.loop.start()
      }
      task.pausing = false
      task.lastRenderTimestamp = getTimestamp()

      logger.info(`task unpaused, taskId: ${task.taskId}`)
    }
  }

  public async updateCanvas(taskId: string, canvas: HTMLCanvasElement | OffscreenCanvas) {
    const task = this.tasks.get(taskId)
    if (task) {
      task.canvas = canvas
      if (task.render) {
        task.render.destroy()
        task.render = null
      }
      task.isSupport = () => false
      task.canvasUpdated = true
      task.firstRendered = false
      task.renderRedyed = false
    }
    else {
      logger.fatal('task not found')
    }
  }

  public async setPlayRate(taskId: string, rate: double) {
    const task = this.tasks.get(taskId)
    if (task) {
      if (task.enableJitterBuffer) {
        let buffer = task.stats.videoPacketQueueLength / task.stats.videoEncodeFramerate * 1000
        if (buffer <= task.stats.jitterBuffer.min) {
          rate = 1
        }
      }
      task.masterTimer.setRate(rate)
      task.playRate = static_cast<int64>(Math.floor(rate * 100))
    }
  }

  public async setRenderMode(taskId: string, mode: RenderMode) {
    const task = this.tasks.get(taskId)
    if (task) {
      if (task.render) {
        task.renderMode = mode
        task.render.setRenderMode(mode)
        nextTick(() => {
          if (task.pausing && task.backFrame && task.render) {
            task.render.render(task.backFrame)
          }
        })
      }
    }
  }

  public async setRenderRotate(taskId: string, rotate: double) {
    const task = this.tasks.get(taskId)
    if (task) {
      if (task.render) {
        task.renderRotate = rotate
        task.render.setRotate(rotate)
        nextTick(() => {
          if (task.pausing && task.backFrame && task.render) {
            task.render.render(task.backFrame)
          }
        })
      }
    }
  }

  public async enableHorizontalFlip(taskId: string, enable: boolean) {
    const task = this.tasks.get(taskId)
    if (task) {
      if (task.render) {
        task.flipHorizontal = enable
        task.render.enableHorizontalFlip(enable)
        nextTick(() => {
          if (task.pausing && task.backFrame && task.render) {
            task.render.render(task.backFrame)
          }
        })
      }
    }
  }

  public async enableVerticalFlip(taskId: string, enable: boolean) {
    const task = this.tasks.get(taskId)
    if (task) {
      if (task.render) {
        task.flipVertical = enable
        task.render.enableVerticalFlip(enable)
        nextTick(() => {
          if (task.pausing && task.backFrame && task.render) {
            task.render.render(task.backFrame)
          }
        })
      }
    }
  }

  public async resize(taskId: string, width: int32, height: int32) {
    const task = this.tasks.get(taskId)
    if (task) {
      if (task.render) {
        task.render.viewport(width, height)
        nextTick(() => {
          if (task.pausing && task.backFrame && task.render) {
            task.render.render(task.backFrame)
          }
        })
      }
    }
  }

  public async setSkipRender(taskId: string, skip: boolean) {
    const task = this.tasks.get(taskId)
    if (task) {
      task.skipRender = skip
      task.stats.videoFrameRenderIntervalMax = 0
      task.lastRenderTimestamp = getTimestamp()
    }
  }

  public async setMasterTime(taskId: string, masterTime: int64) {
    const task = this.tasks.get(taskId)
    if (task) {
      task.masterTimer.setMasterTime(masterTime)
    }
  }

  public async beforeSeek(taskId: string) {
    const task = this.tasks.get(taskId)
    if (task) {
      // 清理历史帧缓存
      if (task.historyFrames) {
        task.historyFrames.clear()
      }

      // 重置帧导航状态
      task.inFrameNavigation = false
      task.playingFromHistory = false

      // 当前正在 pull frontFrame，等待其结束
      if (!task.ended && !task.frontBuffered) {
        logger.debug(`wait current pull front frame before seek, taskId: ${task.taskId}`)
        await new Promise<void>((resolve) => {
          task.seekSync = resolve
        })
      }
      task.seeking = true
      task.loop.stop()
      if (task.backFrame) {
        if (!isPointer(task.backFrame)) {
          task.backFrame.close()
        }
        else {
          task.avframePool.release(task.backFrame)
        }
      }
      if (task.frontFrame) {
        if (!isPointer(task.frontFrame)) {
          task.frontFrame.close()
        }
        else {
          task.avframePool.release(task.frontFrame)
        }
      }

      task.frontFrame = null
      task.backFrame = null
      task.currentPTS = NOPTS_VALUE_BIGINT

      logger.debug(`before seek end, taskId: ${task.taskId}`)
    }
  }

  public async syncSeekTime(taskId: string, timestamp: int64, maxQueueLength?: number) {
    const task = this.tasks.get(taskId)
    if (task) {
      while (true) {
        // 已经调用了 unregisterTask
        if (task.afterPullResolver) {
          task.ended = true
          task.adjust = AdjustStatus.None
          task.backFrame = null
          logger.warn(`pull video frame end after unregisterTask, taskId: ${taskId}`)
          return
        }

        task.backFrame = await task.leftIPCPort.request<pointer<AVFrameRef> | VideoFrame>('pull')

        if (is.number(task.backFrame) && task.backFrame < 0) {
          task.ended = true
          task.adjust = AdjustStatus.None
          task.backFrame = null
          logger.warn(`pull video frame end after seek, taskId: ${taskId}`)
          return
        }

        if (timestamp < 0n) {
          logger.info(`use the first video frame because of the seek time${timestamp} < 0`)
          break
        }

        const pts = isPointer(task.backFrame)
          ? avRescaleQ2(
            task.backFrame.pts,
            addressof(task.backFrame.timeBase),
            AV_MILLI_TIME_BASE_Q
          )
          : avRescaleQ(
            static_cast<int64>(task.backFrame.timestamp as uint32),
            AV_TIME_BASE_Q,
            AV_MILLI_TIME_BASE_Q
          )

        if (pts >= timestamp) {
          task.ended = false
          task.stats.videoCurrentTime = pts
          task.lastNotifyPTS = pts
          task.currentPTS = pts
          task.masterTimer.setMasterTime(timestamp)
          task.lastMasterPts = timestamp
          break
        }

        if (!isPointer(task.backFrame)) {
          if (defined(ENABLE_LOG_TRACE)) {
            logger.trace(`skip video frame pts: ${task.backFrame.timestamp}(${pts}ms), which is earlier then the seeked time(${timestamp}ms), taskId: ${task.taskId}`)
          }
          task.backFrame.close()
        }
        else {
          if (defined(ENABLE_LOG_TRACE)) {
            logger.trace(`skip video frame pts: ${task.backFrame.pts}(${pts}ms), which is earlier then the seeked time(${timestamp}ms), taskId: ${task.taskId}`)
          }
          task.avframePool.release(task.backFrame)
        }
      }
      logger.debug(`sync seek time end, taskId: ${task.taskId}`)
    }
  }

  public async afterSeek(taskId: string, timestamp: int64) {
    const task = this.tasks.get(taskId)
    if (task) {

      if (task.ended) {
        task.seeking = false
        task.controlIPCPort.notify('ended')
        if (task.afterPullResolver) {
          task.afterPullResolver()
          task.afterPullResolver = null
        }
        logger.debug(`after seek end with task ended, taskId: ${task.taskId}`)
        return
      }

      task.frontFrame = await task.leftIPCPort.request<pointer<AVFrameRef> | VideoFrame>('pull')

      if (is.number(task.frontFrame) && task.frontFrame < 0) {
        task.frontFrame = null
        task.frontBuffered = false
        task.ended = true
      }
      else {
        task.frontBuffered = true
        task.ended = false
      }

      if (task.afterPullResolver) {
        if (task.backFrame) {
          if (!isPointer(task.backFrame)) {
            task.backFrame.close()
          }
          else {
            task.avframePool.release(task.backFrame)
          }
          task.backFrame = null
        }
        if (task.frontFrame) {
          if (!isPointer(task.frontFrame)) {
            task.frontFrame.close()
          }
          else {
            task.avframePool.release(task.frontFrame)
          }
          task.frontFrame = null
        }
        task.seeking = false
        task.ended = true
        task.afterPullResolver()
        task.afterPullResolver = null
        logger.debug(`after seek end with unregisterTask call, taskId: ${task.taskId}`)
        return
      }

      task.adjust = AdjustStatus.None
      task.lastRenderTimestamp = getTimestamp()

      task.currentPTS = isPointer(task.backFrame)
        ? avRescaleQ2(
          task.backFrame.pts,
          addressof(task.backFrame.timeBase),
          AV_MILLI_TIME_BASE_Q
        )
        : avRescaleQ(
          static_cast<int64>(task.backFrame.timestamp as uint32),
          AV_TIME_BASE_Q,
          AV_MILLI_TIME_BASE_Q
        )
      task.stats.videoCurrentTime = task.currentPTS

      logger.debug(`got first video frame, pts: ${!isPointer(task.backFrame)
        ? static_cast<int64>(task.backFrame.timestamp as uint32)
        : task.backFrame.pts
      }(${task.currentPTS}ms), taskId: ${task.taskId}`)

      task.seeking = false
      if (!task.pausing) {
        task.loop.start()
      }
      else if (task.backFrame) {
        task.render.render(task.backFrame)
      }

      logger.debug(`after seek end, taskId: ${task.taskId}`)
    }
  }

  /**
 * 向前渲染一帧
 * 在暂停状态下回放到前一帧
 */
  public async renderPrevFrame(taskId: string): Promise<boolean> {
    const task = this.tasks.get(taskId)
    if (!task || !task.render || !task.pausing || !task.historyFrames) {
      return false
    }

    // 标记进入帧导航模式
    task.inFrameNavigation = true

    // 尝试获取前一帧
    const prevFrame = task.historyFrames.prev()
    if (!prevFrame) {
      logger.info(`已经是最早的历史帧，无法向前渲染，taskId: ${task.taskId}`)
      return false
    }

    // 释放当前帧
    if (task.backFrame) {
      if (isPointer(task.backFrame)) {
        task.avframePool.release(task.backFrame)
      }
      else {
        task.backFrame.close()
      }
    }

    // 设置新的当前帧
    task.backFrame = prevFrame

    // 计算 PTS
    const pts = isPointer(task.backFrame)
      ? avRescaleQ2(
        task.backFrame.pts,
        addressof(task.backFrame.timeBase),
        AV_MILLI_TIME_BASE_Q
      )
      : avRescaleQ(
        static_cast<int64>(task.backFrame.timestamp as uint32),
        AV_TIME_BASE_Q,
        AV_MILLI_TIME_BASE_Q
      )

    // 渲染帧
    task.render.render(task.backFrame)

    // 更新状态
    task.stats.videoCurrentTime = pts
    task.stats.videoFrameRenderCount++
    task.currentPTS = pts
    task.lastMasterPts = pts

    logger.debug(`渲染前一帧，位置: ${task.historyFrames.position()}, pts: ${pts}, taskId: ${task.taskId}`)
    return true
  }

  public async renderNextFrame(taskId: string) {
    const task = this.tasks.get(taskId)
    if (task) {
      // 如果在暂停状态和帧导航模式下，尝试从历史帧缓存中获取下一帧
      if (task.pausing && task.inFrameNavigation && task.historyFrames) {
        const nextFrame = task.historyFrames.next()
        if (nextFrame) {
          // 释放当前帧
          if (task.backFrame) {
            if (isPointer(task.backFrame)) {
              task.avframePool.release(task.backFrame)
            }
            else {
              task.backFrame.close()
            }
          }

          // 设置新的当前帧
          task.backFrame = nextFrame

          // 计算 PTS
          const pts = isPointer(task.backFrame)
            ? avRescaleQ2(
              task.backFrame.pts,
              addressof(task.backFrame.timeBase),
              AV_MILLI_TIME_BASE_Q
            )
            : avRescaleQ(
              static_cast<int64>(task.backFrame.timestamp as uint32),
              AV_TIME_BASE_Q,
              AV_MILLI_TIME_BASE_Q
            )

          // 渲染帧
          task.render.render(task.backFrame)

          console.log('渲染下一帧', task.backFrame)
          // 更新状态
          task.stats.videoCurrentTime = pts
          task.stats.videoFrameRenderCount++
          task.currentPTS = pts
          task.lastMasterPts = pts

          logger.debug(`从历史帧渲染下一帧，位置: ${task.historyFrames.position()}, pts: ${pts}, taskId: ${task.taskId}`)
          return true
        }
        else {
          // 已经是最后一帧，退出帧导航模式
          task.inFrameNavigation = false
        }
      }


      if (task.backFrame) {
        const pts = isPointer(task.backFrame)
          ? avRescaleQ2(
            task.backFrame.pts,
            addressof(task.backFrame.timeBase),
            AV_MILLI_TIME_BASE_Q
          )
          : avRescaleQ(
            static_cast<int64>(task.backFrame.timestamp as uint32),
            AV_TIME_BASE_Q,
            AV_MILLI_TIME_BASE_Q
          )
        task.render.render(task.backFrame)
        task.stats.videoCurrentTime = pts
        task.stats.videoFrameRenderCount++
        task.currentPTS = pts
        task.lastMasterPts = pts
        this.swap(task)
      }
    }
  }

  public async registerTask(options: VideoRenderTaskOptions): Promise<number> {
    if (this.tasks.has(options.taskId)) {
      return errorType.INVALID_OPERATE
    }
    return await this.createTask(options)
  }

  public async unregisterTask(id: string): Promise<void> {
    const task = this.tasks.get(id)
    if (task) {
      // 清理历史帧
      if (task.historyFrames) {
        task.historyFrames.clear()
      }

      const started = !!task.loop
      if (task.loop) {
        await task.loop.stopBeforeNextTick()
        task.loop.destroy()
        task.loop = null
      }

      if (started && !task.ended && !task.frontFrame) {
        await new Promise<void>((resolve) => {
          task.afterPullResolver = resolve
        })
      }
      if (task.render) {
        task.render.destroy()
        task.render = null
      }
      if (task.backFrame) {
        if (isPointer(task.backFrame) && task.backFrame > 0) {
          task.avframePool.release(task.backFrame)
        }
        else {
          task.backFrame.close()
        }
        task.backFrame = null
      }
      if (task.frontFrame) {
        if (isPointer(task.frontFrame) && task.frontFrame > 0) {
          task.avframePool.release(task.frontFrame)
        }
        else {
          task.frontFrame.close()
        }
        task.frontFrame = null
      }
      task.leftIPCPort.destroy()
      task.controlIPCPort.destroy()
      this.tasks.delete(id)
    }
  }
}
